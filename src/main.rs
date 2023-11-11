use anyhow::Result;
use axum::{routing::post, Json, Router};
use prql_compiler::{compile, ErrorMessages, Options, Target};
use serde::{Deserialize, Serialize};

mod pipeline;
mod translate;

use pipeline::Pipeline;
use translate::{Dialect, ToPrql};

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

async fn to_prql(Json(request): Json<Request>) -> String {
    request
        .to_prql()
        .expect("Could not convert request to PRQL")
}

async fn to_sql(Json(request): Json<Request>) -> String {
    request.to_sql().expect("Could not convert request to SQL")
}

#[derive(Serialize, Deserialize, Debug)]
struct Request {
    pipeline: Pipeline,
    dialect: Dialect,
}

impl Request {
    fn to_prql(&self) -> Result<String> {
        self.pipeline.to_prql(&self.dialect)
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
