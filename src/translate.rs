use anyhow::Result;
use enum_dispatch::enum_dispatch;
use serde::{Deserialize, Serialize};
pub use serde_json::Value;

use crate::pipeline::*;

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "lowercase")]
pub enum Dialect {
    Postgres,
    BigQuery,
}

#[enum_dispatch]
pub trait ToPrql {
    fn to_prql(&self, dialect: &Dialect) -> Result<String>;
}

#[enum_dispatch]
pub trait ToSString {
    fn to_s_string(&self, dialect: &Dialect) -> Result<String>;
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Column(pub String);

impl ToPrql for Column {
    fn to_prql(&self, _dialect: &Dialect) -> Result<String> {
        Ok(format!("`{}`", self.0))
    }
}

impl ToSString for Column {
    fn to_s_string(&self, dialect: &Dialect) -> Result<String> {
        match dialect {
            Dialect::Postgres => Ok(format!(r#"\"{}\""#, self.0)),
            Dialect::BigQuery => Ok(format!("`{}`", self.0)),
        }
    }
}

impl ToSString for String {
    fn to_s_string(&self, dialect: &Dialect) -> Result<String> {
        // We need to use single quotes for values in s-strings
        // (see https://prql-lang.org/book/reference/syntax/s-strings.html#admonition-note)
        let single_quote_replacement = match dialect {
            Dialect::Postgres => "''",
            Dialect::BigQuery => r#"\\'"#,
        };
        Ok(format!(
            "\'{}\'",
            self.replace('\'', single_quote_replacement)
        ))
    }
}

impl ToSString for Value {
    fn to_s_string(&self, dialect: &Dialect) -> Result<String> {
        match self {
            Value::String(s) => Ok(s.to_s_string(dialect)?),
            _ => Ok(self.to_string()),
        }
    }
}
