use crate::translate::{Column, Dialect, ToPrql};
use anyhow::Result;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct DomainStep {
    domain: Column,
}

impl ToPrql for DomainStep {
    // https://prql-lang.org/book/reference/stdlib/transforms/from.html
    fn to_prql(&self, dialect: &Dialect) -> Result<String> {
        Ok(format!("from {}", self.domain.to_prql(dialect)?))
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
    fn domain_no_space(#[case] dialect: Dialect) {
        let input = json!({
            "domain": "albums"
        });
        assert_eq!(
            serde_json::from_value::<DomainStep>(input)
                .unwrap()
                .to_prql(&dialect)
                .unwrap(),
            "from `albums`"
        );
    }

    #[rstest]
    #[case::postgres(Dialect::Postgres)]
    #[case::bigquery(Dialect::BigQuery)]
    fn domain_with_space(#[case] dialect: Dialect) {
        let input = json!({
            "domain": "alb ums"
        });
        assert_eq!(
            serde_json::from_value::<DomainStep>(input)
                .unwrap()
                .to_prql(&dialect)
                .unwrap(),
            "from `alb ums`"
        );
    }
}
