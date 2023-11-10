use anyhow::Result;
use enum_dispatch::enum_dispatch;
use prql_compiler::{compile, ErrorMessages, Options, Target};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::iter::zip;

#[derive(Serialize, Deserialize, Debug)]
struct Column(String);

impl ToPrql for Column {
    fn to_prql(&self) -> String {
        format!("`{}`", self.0)
    }
}

#[enum_dispatch]
trait ToPrql {
    fn to_prql(&self) -> String;
}

#[derive(Serialize, Deserialize, Debug)]
struct DomainStep {
    domain: String,
    table: bool,
}

impl ToPrql for DomainStep {
    // https://prql-lang.org/book/reference/stdlib/transforms/from.html
    fn to_prql(&self) -> String {
        if self.table {
            format!("from {}", Column(self.domain.clone()).to_prql())
        } else {
            // https://prql-lang.org/book/reference/syntax/s-strings.html
            format!("from s\"{}\"", self.domain)
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
    fn to_prql(&self) -> String {
        match (self.on.len(), self.keep_original_granularity) {
            (0, false) => format!(
                "aggregate {{ {} }}",
                self.aggregations
                    .iter()
                    .map(|agg| agg.to_prql())
                    .collect::<Vec<String>>()
                    .join(", ")
            ),
            (0, true) => format!(
                "derive {{ {} }}",
                self.aggregations
                    .iter()
                    .map(|agg| agg.to_prql())
                    .collect::<Vec<String>>()
                    .join(", ")
            ),
            (_, false) => format!(
                "group {{ {} }} ( aggregate {{ {} }} )",
                self.on
                    .iter()
                    .map(|col| col.to_prql())
                    .collect::<Vec<String>>()
                    .join(", "),
                self.aggregations
                    .iter()
                    .map(|agg| agg.to_prql())
                    .collect::<Vec<String>>()
                    .join(", ")
            ),
            (_, true) => format!(
                // https://prql-lang.org/book/reference/stdlib/transforms/window.html
                "group {{ {} }} ( window rows:.. ( derive {{ {} }} ) )",
                self.on
                    .iter()
                    .map(|col| col.to_prql())
                    .collect::<Vec<String>>()
                    .join(", "),
                self.aggregations
                    .iter()
                    .map(|agg| agg.to_prql())
                    .collect::<Vec<String>>()
                    .join(", ")
            ),
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
    fn to_prql(&self) -> String {
        zip(&self.columns, &self.new_columns)
            .map(|(col, new_col)| {
                format!(
                    "{} = {} {}",
                    new_col.to_prql(),
                    self.function.to_prql(),
                    col.to_prql()
                )
            })
            .collect::<Vec<String>>()
            .join(", ")
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
    fn to_prql(&self) -> String {
        match self {
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
        .to_string()
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "name", rename_all = "lowercase")]
#[enum_dispatch(ToPrql)]
enum PipelineStep {
    Domain(DomainStep),
    Aggregate(AggregateStep),
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

impl ToPrql for Request {
    fn to_prql(&self) -> String {
        self.pipeline
            .iter()
            .map(|step| step.to_prql())
            .collect::<Vec<String>>()
            .join(" | ")
    }
}

impl Request {
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
        compile(&self.to_prql(), &opts)
    }
}

fn main() -> Result<()> {
    // let prql = "from albums | select {title, artist_id}";
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
                "on": ["col1", "col2"],
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
        "dialect": "postgres"
    });
    let request: Request = serde_json::from_value(request)?;
    println!("PRQL: {}", request.to_prql());
    println!("SQL: {}", request.to_sql().unwrap());
    Ok(())
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

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
        assert_eq!(request.to_prql(), "from `albums`");
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
        assert_eq!(request.to_prql(), "from `al bums`");
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
        assert_eq!(request.to_prql(), r#"from s"SELECT * FROM sales""#);
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
            request.to_prql(),
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
            request.to_prql(),
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
            request.to_prql(),
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
            request.to_prql(),
            "from `al bums` | group { `col`, `other col` } ( window rows:.. ( derive { `City` = min `City`, `Price_sum` = sum `Price`, `Somme des quantités` = sum `Quantity` } ) )"
        );
        assert_eq!(request.to_sql().unwrap(), sql);
    }
}
