use std::iter::zip;

use crate::translate::{Column, Dialect, ToPrql};
use anyhow::Result;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct AggregateStep {
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

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use rstest::rstest;
    use serde_json::json;

    use super::*;

    #[rstest]
    #[case::postgres(Dialect::Postgres)]
    #[case::bigquery(Dialect::BigQuery)]
    fn no_group_no_keep_columns(#[case] dialect: Dialect) {
        let input = json!({
            "aggregations": [
                {
                    "columns": [
                        "Price",
                        "Quantity"
                    ],
                    "newcolumns": [
                        "Price_sum",
                        "Somme des quantités"
                    ],
                    "aggfunction": "sum"
                }
            ]
        });
        assert_eq!(
            serde_json::from_value::<AggregateStep>(input)
                .unwrap()
                .to_prql(&dialect)
                .unwrap(),
            "aggregate { `Price_sum` = sum `Price`, `Somme des quantités` = sum `Quantity` }"
        );
    }

    #[rstest]
    #[case::postgres(Dialect::Postgres)]
    #[case::bigquery(Dialect::BigQuery)]
    fn no_group_with_keep_columns(#[case] dialect: Dialect) {
        let input = json!({
            "aggregations": [
                {
                    "columns": [
                        "Price",
                        "Quantity"
                    ],
                    "newcolumns": [
                        "Price_sum",
                        "Somme des quantités"
                    ],
                    "aggfunction": "sum"
                }
            ],
            "keepOriginalGranularity": true
        });
        assert_eq!(
            serde_json::from_value::<AggregateStep>(input)
                .unwrap()
                .to_prql(&dialect)
                .unwrap(),
            "derive { `Price_sum` = sum `Price`, `Somme des quantités` = sum `Quantity` }"
        );
    }

    #[rstest]
    #[case::postgres(Dialect::Postgres)]
    #[case::bigquery(Dialect::BigQuery)]
    fn with_group_no_keep_columns(#[case] dialect: Dialect) {
        let input = json!({
            "on": ["col", "other col"],
            "aggregations": [
                {
                    "columns": [
                        "Price",
                        "Quantity"
                    ],
                    "newcolumns": [
                        "Price_sum",
                        "Somme des quantités"
                    ],
                    "aggfunction": "sum"
                }
            ],
        });
        assert_eq!(
            serde_json::from_value::<AggregateStep>(input)
                .unwrap()
                .to_prql(&dialect)
                .unwrap(),
            "group { `col`, `other col` } ( aggregate { `Price_sum` = sum `Price`, `Somme des quantités` = sum `Quantity` } )"
        );
    }

    #[rstest]
    #[case::postgres(Dialect::Postgres)]
    #[case::bigquery(Dialect::BigQuery)]
    fn with_group_with_keep_columns(#[case] dialect: Dialect) {
        let input = json!({
            "on": ["col", "other col"],
            "aggregations": [
                {
                    "columns": [
                        "Price",
                        "Quantity"
                    ],
                    "newcolumns": [
                        "Price_sum",
                        "Somme des quantités"
                    ],
                    "aggfunction": "sum"
                }
            ],
            "keepOriginalGranularity": true
        });
        assert_eq!(
            serde_json::from_value::<AggregateStep>(input)
                .unwrap()
                .to_prql(&dialect)
                .unwrap(),
            "group { `col`, `other col` } ( window rows:.. ( derive { `Price_sum` = sum `Price`, `Somme des quantités` = sum `Quantity` } ) )"
        );
    }
}
