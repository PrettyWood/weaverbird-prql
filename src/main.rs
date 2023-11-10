use anyhow::Result;
use enum_dispatch::enum_dispatch;
use prql_compiler::{compile, ErrorMessages, Options, Target};
use serde::{Deserialize, Serialize};
use serde_json::json;

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
    fn to_prql(&self) -> String {
        if self.table {
            format!("from `{}`", self.domain)
        } else {
            format!("from s\"{}\"", self.domain)
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "name", rename_all = "lowercase")]
#[enum_dispatch(ToPrql)]
enum PipelineStep {
    Domain(DomainStep),
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "lowercase")]
enum Dialect {
    Postgres,
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
    use super::*;

    #[test]
    fn domain_table() {
        let request = json!(
        {
            "pipeline": [
                {
                    "name": "domain",
                    "domain": "albums",
                    "table": true,
                }
            ],
            "dialect": "postgres"
        });
        let request: Request = serde_json::from_value(request).unwrap();
        assert_eq!(request.to_prql(), "from `albums`");
        assert_eq!(request.to_sql().unwrap(), r#"SELECT * FROM albums"#);
    }

    #[test]
    fn domain_table_with_whitespace() {
        let request = json!(
        {
            "pipeline": [
                {
                    "name": "domain",
                    "domain": "al bums",
                    "table": true,
                }
            ],
            "dialect": "postgres"
        });
        let request: Request = serde_json::from_value(request).unwrap();
        assert_eq!(request.to_prql(), "from `al bums`");
        assert_eq!(request.to_sql().unwrap(), r#"SELECT * FROM "al bums""#);
    }

    #[test]
    fn domain_custom_query() {
        let request = json!(
        {
            "pipeline": [
                {
                    "name": "domain",
                    "domain": "SELECT * FROM sales",
                    "table": false,
                }
            ],
            "dialect": "postgres"
        });
        let request: Request = serde_json::from_value(request).unwrap();
        assert_eq!(request.to_prql(), r#"from s"SELECT * FROM sales""#);
        assert_eq!(
            request.to_sql().unwrap(),
            "WITH table_0 AS (SELECT * FROM sales) SELECT * FROM table_0"
        );
    }
}
