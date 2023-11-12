use crate::translate::{Column, Dialect, ToPrql, ToSString, Value};
use anyhow::Result;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct FilterStep {
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
enum Condition {
    Simple(SimpleCondition),
    Or(OrCondition),
    And(AndCondition),
}

impl ToPrql for Condition {
    // Sad but enum_dispatch does not work across crates
    fn to_prql(&self, dialect: &Dialect) -> Result<String> {
        match self {
            Condition::Simple(condition) => condition.to_prql(dialect),
            Condition::Or(condition) => condition.to_prql(dialect),
            Condition::And(condition) => condition.to_prql(dialect),
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
enum SimpleCondition {
    Comparison(ComparisonCondition),
    Nullability(NullabilityCondition),
    Inclusion(InclusionCondition),
    Matches(MatchesCondition),
    // Date(DateCondition),
}

impl ToPrql for SimpleCondition {
    // Sad but enum_dispatch does not work across crates
    fn to_prql(&self, dialect: &Dialect) -> Result<String> {
        match self {
            SimpleCondition::Comparison(condition) => condition.to_prql(dialect),
            SimpleCondition::Nullability(condition) => condition.to_prql(dialect),
            SimpleCondition::Inclusion(condition) => condition.to_prql(dialect),
            SimpleCondition::Matches(condition) => condition.to_prql(dialect),
            // SimpleCondition::Date(condition) => condition.to_prql(dialect),
        }
    }
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
            .collect::<Result<Vec<String>>>()
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
struct MatchesCondition {
    column: Column,
    operator: MatchesOperator,
    value: String,
}

impl ToPrql for MatchesCondition {
    fn to_prql(&self, dialect: &Dialect) -> Result<String> {
        match (&self.operator, dialect) {
            (MatchesOperator::Matches, Dialect::Postgres) => Ok(format!(
                r#"s"{} SIMILAR TO {}""#,
                self.column.to_s_string(dialect)?,
                self.value.to_s_string(dialect)?,
            )),
            (MatchesOperator::NotMatches, Dialect::Postgres) => Ok(format!(
                r#"s"{} NOT SIMILAR TO {}""#,
                self.column.to_s_string(dialect)?,
                self.value.to_s_string(dialect)?,
            )),
            (MatchesOperator::Matches, Dialect::BigQuery) => Ok(format!(
                r#"s"REGEXP_CONTAINS({},{})""#,
                self.column.to_s_string(dialect)?,
                self.value.to_s_string(dialect)?,
            )),
            (MatchesOperator::NotMatches, Dialect::BigQuery) => Ok(format!(
                r#"s"NOT REGEXP_CONTAINS({},{})""#,
                self.column.to_s_string(dialect)?,
                self.value.to_s_string(dialect)?,
            )),
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "lowercase")]
enum MatchesOperator {
    Matches,
    NotMatches,
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

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use rstest::rstest;
    use serde_json::json;

    use super::*;

    #[rstest]
    #[case::postgres(Dialect::Postgres)]
    #[case::bigquery(Dialect::BigQuery)]
    fn filter_basic(#[case] dialect: Dialect) {
        let input = json!(
        {
            "condition": {
                "column": "City",
                "value": "Paris",
                "operator": "eq"
            }
        });
        assert_eq!(
            serde_json::from_value::<FilterStep>(input)
                .unwrap()
                .to_prql(&dialect)
                .unwrap(),
            r#"filter `City` == "Paris""#
        );
    }

    #[rstest]
    #[case::postgres(Dialect::Postgres)]
    #[case::bigquery(Dialect::BigQuery)]
    fn filter_or(#[case] dialect: Dialect) {
        let input = json!(
        {
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
        });
        assert_eq!(
            serde_json::from_value::<FilterStep>(input)
                .unwrap()
                .to_prql(&dialect)
                .unwrap(),
            r#"filter (`my value` < 42 || `my other value` >= 53)"#
        );
    }

    #[rstest]
    #[case::postgres(Dialect::Postgres)]
    #[case::bigquery(Dialect::BigQuery)]
    fn filter_and(#[case] dialect: Dialect) {
        let input = json!(
        {
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
        });
        assert_eq!(
            serde_json::from_value::<FilterStep>(input)
                .unwrap()
                .to_prql(&dialect)
                .unwrap(),
            r#"filter `my value` < 42 && `my other value` >= 53"#
        );
    }

    #[rstest]
    #[case::postgres(
        Dialect::Postgres,
        r#"filter `val1` != null && `val2` == null && (s"\"color\" IN ('blue', 'red')" || s"\"ma destination\" NOT IN ('l''aéroport', 'la gare')" || s"\"my value\" IN (1, null)" || `color3` == "green")"#
    )]
    #[case::bigquery(
        Dialect::BigQuery,
        r#"filter `val1` != null && `val2` == null && (s"`color` IN ('blue', 'red')" || s"`ma destination` NOT IN ('l\\'aéroport', 'la gare')" || s"`my value` IN (1, null)" || `color3` == "green")"#
    )]
    fn filter_complex(#[case] dialect: Dialect, #[case] prql: &str) {
        let input = json!(
        {
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
                                "column": "my value",
                                "operator": "in",
                                "value": [1, null],
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
        });
        assert_eq!(
            serde_json::from_value::<FilterStep>(input)
                .unwrap()
                .to_prql(&dialect)
                .unwrap(),
            prql
        );
    }

    #[rstest]
    #[case::postgres(
        Dialect::Postgres,
        r#"filter s"\"val1\" SIMILAR TO 'pika'" && s"\"val2\" NOT SIMILAR TO 'chu'""#
    )]
    #[case::bigquery(
        Dialect::BigQuery,
        r#"filter s"REGEXP_CONTAINS(`val1`,'pika')" && s"NOT REGEXP_CONTAINS(`val2`,'chu')""#
    )]
    fn filter_complex_2(#[case] dialect: Dialect, #[case] prql: &str) {
        let input = json!(
        {
            "condition": {
                "and": [
                    {
                        "column": "val1",
                        "operator": "matches",
                        "value": "pika",
                    },
                    {
                        "column": "val2",
                        "operator": "notmatches",
                        "value": "chu",
                    },
                ]
            }
        });
        assert_eq!(
            serde_json::from_value::<FilterStep>(input)
                .unwrap()
                .to_prql(&dialect)
                .unwrap(),
            prql
        );
    }
}
