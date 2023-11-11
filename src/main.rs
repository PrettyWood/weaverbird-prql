use anyhow::Result;
use axum::{routing::post, Json, Router};
use enum_dispatch::enum_dispatch;
use prql_compiler::{compile, ErrorMessages, Options, Target};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::iter::zip;

#[enum_dispatch]
trait ToPrql {
    fn to_prql(&self, dialect: &Dialect) -> Result<String>;
}

#[enum_dispatch]
trait ToSString {
    fn to_s_string(&self, dialect: &Dialect) -> Result<String, ErrorMessages>;
}

impl ToSString for Value {
    fn to_s_string(&self, dialect: &Dialect) -> Result<String, ErrorMessages> {
        // We need to use single quotes for values in s-strings
        // (see https://prql-lang.org/book/reference/syntax/s-strings.html#admonition-note)
        let single_quote_replacement = match dialect {
            Dialect::Postgres => "''",
            Dialect::BigQuery => r#"\\'"#,
        };
        match self {
            Value::String(s) => Ok(format!("\'{}\'", s.replace('\'', single_quote_replacement))),
            _ => Ok(self.to_string()),
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct Column(String);

impl ToPrql for Column {
    fn to_prql(&self, _dialect: &Dialect) -> Result<String> {
        Ok(format!("`{}`", self.0))
    }
}

impl ToSString for Column {
    fn to_s_string(&self, dialect: &Dialect) -> Result<String, ErrorMessages> {
        match dialect {
            Dialect::Postgres => Ok(format!(r#"\"{}\""#, self.0)),
            Dialect::BigQuery => Ok(format!("`{}`", self.0)),
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct DomainStep {
    domain: String,
    table: bool,
}

impl ToPrql for DomainStep {
    // https://prql-lang.org/book/reference/stdlib/transforms/from.html
    fn to_prql(&self, dialect: &Dialect) -> Result<String> {
        if self.table {
            Ok(format!(
                "from {}",
                Column(self.domain.clone()).to_prql(dialect)?
            ))
        } else {
            // https://prql-lang.org/book/reference/syntax/s-strings.html
            Ok(format!("from s\"{}\"", self.domain))
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct AggregateStep {
    #[serde(default)]
    on: Vec<Column>,
    aggregations: Vec<Aggregation>,
    #[serde(rename = "keepOriginalGranularity")]
    #[serde(default)]
    keep_original_granularity: bool,
}

impl ToPrql for AggregateStep {
    // https://prql-lang.org/book/reference/stdlib/transforms/aggregate.html
    fn to_prql(&self, dialect: &Dialect) -> Result<String> {
        match (self.on.len(), self.keep_original_granularity) {
            (0, false) => Ok(format!(
                "aggregate {{ {} }}",
                self.aggregations
                    .iter()
                    .map(|agg| agg.to_prql(dialect))
                    .collect::<Result<Vec<String>>>()?
                    .join(", ")
            )),
            (0, true) => Ok(format!(
                "derive {{ {} }}",
                self.aggregations
                    .iter()
                    .map(|agg| agg.to_prql(dialect))
                    .collect::<Result<Vec<String>>>()?
                    .join(", ")
            )),
            (_, false) => Ok(format!(
                "group {{ {} }} ( aggregate {{ {} }} )",
                self.on
                    .iter()
                    .map(|col| col.to_prql(dialect))
                    .collect::<Result<Vec<String>>>()?
                    .join(", "),
                self.aggregations
                    .iter()
                    .map(|agg| agg.to_prql(dialect))
                    .collect::<Result<Vec<String>>>()?
                    .join(", ")
            )),
            (_, true) => Ok(format!(
                // https://prql-lang.org/book/reference/stdlib/transforms/window.html
                "group {{ {} }} ( window rows:.. ( derive {{ {} }} ) )",
                self.on
                    .iter()
                    .map(|col| col.to_prql(dialect))
                    .collect::<Result<Vec<String>>>()?
                    .join(", "),
                self.aggregations
                    .iter()
                    .map(|agg| agg.to_prql(dialect))
                    .collect::<Result<Vec<String>>>()?
                    .join(", ")
            )),
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct Aggregation {
    pub columns: Vec<Column>,
    #[serde(rename = "newcolumns")]
    pub new_columns: Vec<Column>,
    #[serde(rename = "aggfunction")]
    pub function: AggregationFn,
}

impl ToPrql for Aggregation {
    fn to_prql(&self, dialect: &Dialect) -> Result<String> {
        Ok(zip(&self.columns, &self.new_columns)
            .map(|(col, new_col)| {
                Ok(format!(
                    "{} = {} {}",
                    new_col.to_prql(dialect)?,
                    self.function.to_prql(dialect)?,
                    col.to_prql(dialect).unwrap(),
                ))
            })
            .collect::<Result<Vec<String>>>()?
            .join(", "))
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "lowercase")]
enum AggregationFn {
    Min,
    Max,
    Count,
    Avg,
    Sum,
    #[serde(rename = "count distinct")]
    CountDistinct,
    First,
    Last,
}

impl ToPrql for AggregationFn {
    fn to_prql(&self, _dialect: &Dialect) -> Result<String> {
        Ok(match self {
            AggregationFn::Min => "min",
            AggregationFn::Max => "max",
            AggregationFn::Count => "count",
            AggregationFn::Avg => "avg",
            AggregationFn::Sum => "sum",
            AggregationFn::CountDistinct => "count_distinct",
            // https://prql-lang.org/book/reference/stdlib/transforms/aggregate.html#admonition-note
            // Those two are not implemented in PRQL, but we can use min and max
            AggregationFn::First => "min",
            AggregationFn::Last => "max",
        }
        .to_string())
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct FilterStep {
    condition: Condition,
}

impl ToPrql for FilterStep {
    // https://prql-lang.org/book/reference/stdlib/transforms/filter.html
    fn to_prql(&self, dialect: &Dialect) -> Result<String> {
        Ok(format!("filter {}", self.condition.to_prql(dialect)?))
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
#[enum_dispatch(ToPrql)]
enum Condition {
    Simple(SimpleCondition),
    Or(OrCondition),
    And(AndCondition),
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
#[enum_dispatch(ToPrql)]
enum SimpleCondition {
    Comparison(ComparisonCondition),
    Nullability(NullabilityCondition),
    Inclusion(InclusionCondition),
    // Matches(MatchesCondition),
    // Date(DateCondition),
}

#[derive(Serialize, Deserialize, Debug)]
struct ComparisonCondition {
    column: Column,
    operator: ComparisonOperator,
    value: Value,
}

impl ToPrql for ComparisonCondition {
    fn to_prql(&self, dialect: &Dialect) -> Result<String> {
        let op = match self.operator {
            ComparisonOperator::Eq => "==",
            ComparisonOperator::Ne => "!=",
            ComparisonOperator::Gt => ">",
            ComparisonOperator::Gte => ">=",
            ComparisonOperator::Lt => "<",
            ComparisonOperator::Lte => "<=",
        };
        Ok(format!(
            "{} {} {}",
            self.column.to_prql(dialect)?,
            op,
            self.value
        ))
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "lowercase")]
enum ComparisonOperator {
    Eq,
    Ne,
    Gt,
    Gte,
    Lt,
    Lte,
}

#[derive(Serialize, Deserialize, Debug)]
struct NullabilityCondition {
    column: Column,
    #[serde(rename = "operator")]
    operator: NullabilityOperator,
}

impl ToPrql for NullabilityCondition {
    fn to_prql(&self, dialect: &Dialect) -> Result<String> {
        match self.operator {
            NullabilityOperator::IsNull => Ok(format!("{} == null", self.column.to_prql(dialect)?)),
            NullabilityOperator::NotNull => {
                Ok(format!("{} != null", self.column.to_prql(dialect)?))
            }
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "lowercase")]
enum NullabilityOperator {
    IsNull,
    NotNull,
}

#[derive(Serialize, Deserialize, Debug)]
struct InclusionCondition {
    column: Column,
    #[serde(rename = "operator")]
    operator: InclusionOperator,
    value: Vec<Value>,
}

impl ToPrql for InclusionCondition {
    // IN is not yet supported in PRQL (see https://github.com/PRQL/prql/issues/993)
    // We hence rely on s-strings (https://prql-lang.org/book/reference/syntax/s-strings.html)
    fn to_prql(&self, dialect: &Dialect) -> Result<String> {
        let joined_values = self
            .value
            .iter()
            .map(|v| v.to_s_string(dialect))
            .collect::<Result<Vec<String>, ErrorMessages>>()
            .map(|v| v.join(", "));
        match self.operator {
            InclusionOperator::In => Ok(format!(
                r#"s"{} IN ({})""#,
                self.column.to_s_string(dialect)?,
                joined_values?
            )),
            InclusionOperator::Nin => Ok(format!(
                r#"s"{} NOT IN ({})""#,
                self.column.to_s_string(dialect)?,
                joined_values?
            )),
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "lowercase")]
enum InclusionOperator {
    In,
    Nin,
}

#[derive(Serialize, Deserialize, Debug)]
struct OrCondition {
    or: Vec<Condition>,
}

impl ToPrql for OrCondition {
    fn to_prql(&self, dialect: &Dialect) -> Result<String> {
        Ok(format!(
            "({})",
            self.or
                .iter()
                .map(|condition| condition.to_prql(dialect))
                .collect::<Result<Vec<String>>>()?
                .join(" || ")
        ))
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct AndCondition {
    and: Vec<Condition>,
}

impl ToPrql for AndCondition {
    fn to_prql(&self, dialect: &Dialect) -> Result<String> {
        Ok(self
            .and
            .iter()
            .map(|condition| condition.to_prql(dialect))
            .collect::<Result<Vec<String>>>()?
            .join(" && "))
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "name", rename_all = "lowercase")]
#[enum_dispatch(ToPrql)]
enum PipelineStep {
    Domain(DomainStep),
    Aggregate(AggregateStep),
    Filter(FilterStep),
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "lowercase")]
enum Dialect {
    Postgres,
    BigQuery,
}

#[derive(Serialize, Deserialize, Debug)]
struct Request {
    pipeline: Vec<PipelineStep>,
    dialect: Dialect,
}

impl Request {
    fn to_prql(&self) -> Result<String> {
        Ok(self
            .pipeline
            .iter()
            .map(|step| step.to_prql(&self.dialect))
            .collect::<Result<Vec<String>>>()?
            .join(" | "))
    }

    fn to_sql(&self) -> Result<String, ErrorMessages> {
        let target = match self.dialect {
            Dialect::Postgres => Target::Sql(Some(prql_compiler::sql::Dialect::Postgres)),
            Dialect::BigQuery => Target::Sql(Some(prql_compiler::sql::Dialect::BigQuery)),
        };
        let opts = Options {
            format: false,
            target,
            signature_comment: false,
            color: false,
        };
        compile(&self.to_prql()?, &opts)
    }
}

async fn to_prql(Json(request): Json<Request>) -> String {
    request
        .to_prql()
        .expect("Could not convert request to PRQL")
}

async fn to_sql(Json(request): Json<Request>) -> String {
    request.to_sql().expect("Could not convert request to SQL")
}

#[tokio::main]
async fn main() {
    let app = Router::new()
        .route("/prql", post(to_prql))
        .route("/sql", post(to_sql));
    axum::Server::bind(&"0.0.0.0:3000".parse().unwrap())
        .serve(app.into_make_service())
        .await
        .unwrap();
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use rstest::rstest;
    use serde_json::json;

    use super::*;

    #[rstest]
    #[case::postgres("postgres")]
    #[case::bigquery("bigquery")]
    fn domain_table(#[case] dialect: &str) {
        let request = json!(
        {
            "pipeline": [
                {
                    "name": "domain",
                    "domain": "albums",
                    "table": true,
                }
            ],
            "dialect": dialect
        });
        let request: Request = serde_json::from_value(request).unwrap();
        assert_eq!(request.to_prql().unwrap(), "from `albums`");
        assert_eq!(request.to_sql().unwrap(), "SELECT * FROM albums");
    }

    #[rstest]
    #[case::postgres("postgres", "SELECT * FROM \"al bums\"")]
    #[case::bigquery("bigquery", "SELECT * FROM `al bums`")]
    fn domain_table_with_whitespace(#[case] dialect: &str, #[case] sql: &str) {
        let request = json!(
        {
            "pipeline": [
                {
                    "name": "domain",
                    "domain": "al bums",
                    "table": true,
                }
            ],
            "dialect": dialect
        });
        let request: Request = serde_json::from_value(request).unwrap();
        assert_eq!(request.to_prql().unwrap(), "from `al bums`");
        assert_eq!(request.to_sql().unwrap(), sql);
    }

    #[rstest]
    #[case::postgres("postgres")]
    #[case::bigquery("bigquery")]
    fn domain_custom_query(#[case] dialect: &str) {
        let request = json!(
        {
            "pipeline": [
                {
                    "name": "domain",
                    "domain": "SELECT * FROM sales",
                    "table": false,
                }
            ],
            "dialect": dialect
        });
        let request: Request = serde_json::from_value(request).unwrap();
        assert_eq!(request.to_prql().unwrap(), r#"from s"SELECT * FROM sales""#);
        assert_eq!(
            request.to_sql().unwrap(),
            "WITH table_0 AS (SELECT * FROM sales) SELECT * FROM table_0"
        );
    }

    #[rstest]
    #[case::postgres("postgres", r#"SELECT MIN("City") AS "City", COALESCE(SUM("Price"), 0) AS "Price_sum", COALESCE(SUM("Quantity"), 0) AS "Somme des quantités" FROM "al bums""#)]
    #[case::bigquery("bigquery", "SELECT MIN(`City`) AS `City`, COALESCE(SUM(`Price`), 0) AS `Price_sum`, COALESCE(SUM(`Quantity`), 0) AS `Somme des quantités` FROM `al bums`")]
    fn aggregation_group_no_keep_no(#[case] dialect: &str, #[case] sql: &str) {
        let request = json!(
        {
            "pipeline": [
                {
                    "name": "domain",
                    "domain": "al bums",
                    "table": true,
                },
                {
                    "name": "aggregate",
                    "aggregations": [
                        {
                            "columns": ["City"],
                            "newcolumns": ["City"],
                            "aggfunction": "first",
                        },
                        {
                            "columns": ["Price", "Quantity"],
                            "newcolumns": ["Price_sum", "Somme des quantités"],
                            "aggfunction": "sum",
                        },
                    ],
                }
            ],
            "dialect": dialect,
        });
        let request: Request = serde_json::from_value(request).unwrap();
        assert_eq!(
            request.to_prql().unwrap(),
            r#"from `al bums` | aggregate { `City` = min `City`, `Price_sum` = sum `Price`, `Somme des quantités` = sum `Quantity` }"#
        );
        assert_eq!(request.to_sql().unwrap(), sql);
    }

    #[rstest]
    #[case::postgres("postgres", r#"SELECT *, MIN("City") OVER () AS "City", SUM("Price") OVER () AS "Price_sum", SUM("Quantity") OVER () AS "Somme des quantités" FROM "al bums""#)]
    #[case::bigquery("bigquery", "SELECT *, MIN(`City`) OVER () AS `City`, SUM(`Price`) OVER () AS `Price_sum`, SUM(`Quantity`) OVER () AS `Somme des quantités` FROM `al bums`")]
    fn aggregation_group_no_keep_yes(#[case] dialect: &str, #[case] sql: &str) {
        let request = json!(
        {
            "pipeline": [
                {
                    "name": "domain",
                    "domain": "al bums",
                    "table": true,
                },
                {
                    "name": "aggregate",
                    "aggregations": [
                        {
                            "columns": ["City"],
                            "newcolumns": ["City"],
                            "aggfunction": "first",
                        },
                        {
                            "columns": ["Price", "Quantity"],
                            "newcolumns": ["Price_sum", "Somme des quantités"],
                            "aggfunction": "sum",
                        },
                    ],
                    "keepOriginalGranularity": true,
                }
            ],
            "dialect": dialect,
        });
        let request: Request = serde_json::from_value(request).unwrap();
        assert_eq!(
            request.to_prql().unwrap(),
            "from `al bums` | derive { `City` = min `City`, `Price_sum` = sum `Price`, `Somme des quantités` = sum `Quantity` }"
        );
        assert_eq!(request.to_sql().unwrap(), sql);
    }

    #[rstest]
    #[case::postgres("postgres", r#"SELECT col, "other col", MIN("City") AS "City", COALESCE(SUM("Price"), 0) AS "Price_sum", COALESCE(SUM("Quantity"), 0) AS "Somme des quantités" FROM "al bums" GROUP BY col, "other col""#)]
    #[case::bigquery("bigquery", "SELECT col, `other col`, MIN(`City`) AS `City`, COALESCE(SUM(`Price`), 0) AS `Price_sum`, COALESCE(SUM(`Quantity`), 0) AS `Somme des quantités` FROM `al bums` GROUP BY col, `other col`")]
    fn aggregation_group_yes_keep_no(#[case] dialect: &str, #[case] sql: &str) {
        let request = json!(
        {
            "pipeline": [
                {
                    "name": "domain",
                    "domain": "al bums",
                    "table": true,
                },
                {
                    "name": "aggregate",
                    "on": ["col", "other col"],
                    "aggregations": [
                        {
                            "columns": ["City"],
                            "newcolumns": ["City"],
                            "aggfunction": "first",
                        },
                        {
                            "columns": ["Price", "Quantity"],
                            "newcolumns": ["Price_sum", "Somme des quantités"],
                            "aggfunction": "sum",
                        },
                    ],
                }
            ],
            "dialect": dialect,
        });
        let request: Request = serde_json::from_value(request).unwrap();
        assert_eq!(
            request.to_prql().unwrap(),
            "from `al bums` | group { `col`, `other col` } ( aggregate { `City` = min `City`, `Price_sum` = sum `Price`, `Somme des quantités` = sum `Quantity` } )"
        );
        assert_eq!(request.to_sql().unwrap(), sql);
    }

    #[rstest]
    #[case::postgres("postgres", r#"SELECT *, MIN("City") OVER (PARTITION BY col, "other col") AS "City", SUM("Price") OVER (PARTITION BY col, "other col") AS "Price_sum", SUM("Quantity") OVER (PARTITION BY col, "other col") AS "Somme des quantités" FROM "al bums""#)]
    #[case::bigquery("bigquery", "SELECT *, MIN(`City`) OVER (PARTITION BY col, `other col`) AS `City`, SUM(`Price`) OVER (PARTITION BY col, `other col`) AS `Price_sum`, SUM(`Quantity`) OVER (PARTITION BY col, `other col`) AS `Somme des quantités` FROM `al bums`")]
    fn aggregation_group_yes_keep_yes(#[case] dialect: &str, #[case] sql: &str) {
        let request = json!(
        {
            "pipeline": [
                {
                    "name": "domain",
                    "domain": "al bums",
                    "table": true,
                },
                {
                    "name": "aggregate",
                    "on": ["col", "other col"],
                    "aggregations": [
                        {
                            "columns": ["City"],
                            "newcolumns": ["City"],
                            "aggfunction": "first",
                        },
                        {
                            "columns": ["Price", "Quantity"],
                            "newcolumns": ["Price_sum", "Somme des quantités"],
                            "aggfunction": "sum",
                        },
                    ],
                    "keepOriginalGranularity": true,
                }
            ],
            "dialect": dialect,
        });
        let request: Request = serde_json::from_value(request).unwrap();
        assert_eq!(
            request.to_prql().unwrap(),
            "from `al bums` | group { `col`, `other col` } ( window rows:.. ( derive { `City` = min `City`, `Price_sum` = sum `Price`, `Somme des quantités` = sum `Quantity` } ) )"
        );
        assert_eq!(request.to_sql().unwrap(), sql);
    }

    #[rstest]
    #[case::postgres("postgres", r#"SELECT * FROM "al bums" WHERE "City" = 'Paris'"#)]
    #[case::bigquery("bigquery", "SELECT * FROM `al bums` WHERE `City` = 'Paris'")]
    fn filter_basic(#[case] dialect: &str, #[case] sql: &str) {
        let request = json!(
        {
            "pipeline": [
                {
                    "name": "domain",
                    "domain": "al bums",
                    "table": true,
                },
                {
                    "name": "filter",
                    "condition": {
                      "column": "City",
                      "value": "Paris",
                      "operator": "eq"
                    }
                }
            ],
            "dialect": dialect
        });
        let request: Request = serde_json::from_value(request).unwrap();
        assert_eq!(
            request.to_prql().unwrap(),
            r#"from `al bums` | filter `City` == "Paris""#
        );
        assert_eq!(request.to_sql().unwrap(), sql);
    }

    #[rstest]
    #[case::postgres(
        "postgres",
        r#"SELECT * FROM "al bums" WHERE "my value" < 42 OR "my other value" >= 53"#
    )]
    #[case::bigquery(
        "bigquery",
        "SELECT * FROM `al bums` WHERE `my value` < 42 OR `my other value` >= 53"
    )]
    fn filter_or(#[case] dialect: &str, #[case] sql: &str) {
        let request = json!(
        {
            "pipeline": [
                {
                    "name": "domain",
                    "domain": "al bums",
                    "table": true,
                },
                {
                    "name": "filter",
                    "condition": {
                        "or": [
                            {
                                "column": "my value",
                                "operator": "lt",
                                "value": 42
                            },
                            {
                                "column": "my other value",
                                "operator": "gte",
                                "value": 53
                            }
                        ]
                    }
                }
            ],
            "dialect": dialect
        });
        let request: Request = serde_json::from_value(request).unwrap();
        assert_eq!(
            request.to_prql().unwrap(),
            r#"from `al bums` | filter (`my value` < 42 || `my other value` >= 53)"#
        );
        assert_eq!(request.to_sql().unwrap(), sql);
    }

    #[rstest]
    #[case::postgres(
        "postgres",
        r#"SELECT * FROM "al bums" WHERE "my value" < 42 AND "my other value" >= 53"#
    )]
    #[case::bigquery(
        "bigquery",
        "SELECT * FROM `al bums` WHERE `my value` < 42 AND `my other value` >= 53"
    )]
    fn filter_and(#[case] dialect: &str, #[case] sql: &str) {
        let request = json!(
        {
            "pipeline": [
                {
                    "name": "domain",
                    "domain": "al bums",
                    "table": true,
                },
                {
                    "name": "filter",
                    "condition": {
                        "and": [
                            {
                                "column": "my value",
                                "operator": "lt",
                                "value": 42
                            },
                            {
                                "column": "my other value",
                                "operator": "gte",
                                "value": 53
                            }
                        ]
                    }
                }
            ],
            "dialect": dialect
        });
        let request: Request = serde_json::from_value(request).unwrap();
        assert_eq!(
            request.to_prql().unwrap(),
            r#"from `al bums` | filter `my value` < 42 && `my other value` >= 53"#
        );
        assert_eq!(request.to_sql().unwrap(), sql);
    }

    #[rstest]
    #[case::postgres(
        "postgres",
        r#"from `al bums` | filter `val1` != null && `val2` == null && (s"\"color\" IN ('blue', 'red')" || s"\"ma destination\" NOT IN ('l''aéroport', 'la gare')" || `color3` == "green")"#,
        r#"SELECT * FROM "al bums" WHERE val1 IS NOT NULL AND val2 IS NULL AND ("color" IN ('blue', 'red') OR "ma destination" NOT IN ('l''aéroport', 'la gare') OR color3 = 'green')"#
    )]
    #[case::bigquery(
        "bigquery",
        r#"from `al bums` | filter `val1` != null && `val2` == null && (s"`color` IN ('blue', 'red')" || s"`ma destination` NOT IN ('l\\'aéroport', 'la gare')" || `color3` == "green")"#,
        r#"SELECT * FROM `al bums` WHERE val1 IS NOT NULL AND val2 IS NULL AND (`color` IN ('blue', 'red') OR `ma destination` NOT IN ('l\'aéroport', 'la gare') OR color3 = 'green')"#
    )]
    fn filter_complex(#[case] dialect: &str, #[case] prql: &str, #[case] sql: &str) {
        let request = json!(
        {
            "pipeline": [
                {
                    "name": "domain",
                    "domain": "al bums",
                    "table": true,
                },
                {
                    "name": "filter",
                    "condition": {
                        "and": [
                          {
                            "column": "val1",
                            "operator": "notnull"
                          },
                          {
                            "column": "val2",
                            "operator": "isnull"
                          },
                          {
                            "or": [
                              {
                                "column": "color",
                                "operator": "in",
                                "value": ["blue", "red"],
                              },
                              {
                                "column": "ma destination",
                                "operator": "nin",
                                "value": ["l'aéroport", "la gare"],
                              },
                              {
                                "column": "color3",
                                "operator": "eq",
                                "value": "green",
                              }
                            ]
                          }
                        ]
                      }
                }
            ],
            "dialect": dialect
        });
        let request: Request = serde_json::from_value(request).unwrap();
        assert_eq!(request.to_prql().unwrap(), prql);
        assert_eq!(request.to_sql().unwrap(), sql);
    }
}
