use std::sync::Arc;

use anyhow::bail;

use crate::db::db::Db;

pub fn parse_sql(sql: String, db: Arc<Db>) -> Result<String, anyhow::Error> {
    let sql = sql.split(" ").collect::<Vec<_>>();

    match (sql.contains(&&"COUNT(*)"), sql.contains(&&"SELECT")) {
        (true, true) => {
            let table_name = sql.last().expect("last failed: parser");
            Ok(db
                .get_schema_page()
                .get_cell_count_page_schema(table_name.to_string())
                .expect("fail")
                .to_string())
        }
        _ => {
            bail!(format!("invalid command {:?}", sql))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn get_db_instance() -> Db {
        let db_file_path: String =
            "/Users/ollaid/codecrafters/codecrafters-sqlite-rust/sample.db".into();
        let db = Db::new(db_file_path.clone());
        db
    }

    #[test]
    fn test_parser_sql() {
        let db = Arc::new(get_db_instance());
        let sql_apples = "SELECT COUNT(*) FROM apples";
        let sql_oranges = "SELECT COUNT(*) FROM oranges";
        let res_apples = parse_sql(sql_apples.to_string(), db.clone()).expect("PARSE FAILED");
        let res_oranges = parse_sql(sql_oranges.to_string(), db.clone()).expect("PARSE FAILED");
        assert_eq!(res_apples, "4");
        assert_eq!(res_oranges, "6")
    }
}
