use prql_compiler::{compile, sql::Dialect, ErrorMessages, Options, Target};

pub fn translate(prql: &str, dialect: Dialect) -> Result<String, ErrorMessages> {
    let opts = Options {
        format: false,
        target: Target::Sql(Some(dialect)),
        signature_comment: false,
        color: false,
    };
    compile(prql, &opts)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let prql = "from albums | select {title, artist_id}";
        let res = translate(prql, prql_compiler::sql::Dialect::BigQuery);
        assert_eq!(res.unwrap(), "SELECT title, artist_id FROM albums");
    }
}
