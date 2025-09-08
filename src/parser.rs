use std::sync::Arc;

use anyhow::bail;

use crate::db::db::Db;

pub fn parse_sql(sql: String, db: Arc<Db>) -> Result<usize, anyhow::Error> {
    let sql = sql.split(" ").map(|c| c.to_lowercase()).collect::<Vec<_>>();
    //println!("{:?}", db.get_schema_page());
    match (
        sql.contains(&"count(*)".to_string()),
        sql.contains(&"select".to_string()),
    ) {
        (true, true) => {
            let table_name = sql.last().expect("last failed: parser");
            Ok(db
                .get_schema_page()
                .borrow()
                .get_cell_count_page_schema(table_name.to_string())
                .expect("fail"))
        }
        _ => {
            bail!(format!("invalid command {:?}", sql))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn get_db_instance(db_name: String) -> Db {
        let db_file_path: String =
            format!("/Users/ollaid/codecrafters/codecrafters-sqlite-rust/{db_name}.db");
        let db = Db::new(db_file_path.clone());
        db
    }

    #[test]
    fn test_parser_sql() {
        let db = Arc::new(get_db_instance("sample".into()));
        let sql_apples = "SELECT COUNT(*) FROM apples";
        let sql_oranges = "SELECT COUNT(*) FROM oranges";
        let res_apples = parse_sql(sql_apples.to_string(), db.clone()).expect("PARSE FAILED");
        let res_oranges = parse_sql(sql_oranges.to_string(), db.clone()).expect("PARSE FAILED");
        assert_eq!(res_apples, 4);
        assert_eq!(res_oranges, 6)
    }

    #[test]
    fn test_parser_sql_superhoreos() {
        let db_name: String = "superheroes".into();

        let db = Arc::new(get_db_instance(db_name.clone()));
        let sql_ = format!("SELECT COUNT(*) FROM {db_name}");
        let res_ = parse_sql(sql_.to_string(), db.clone()).expect("PARSE FAILED!!");
        assert_eq!(res_, 108);
    }

    #[test]
    fn test_parser_sql_companies() {
        let db_name: String = "companies".into();

        let db = Arc::new(get_db_instance(db_name.clone()));
        let sql_ = format!("SELECT COUNT(*) FROM {db_name}");
        let res_ = parse_sql(sql_.to_string(), db.clone()).expect("PARSE FAILED!!");
        assert_eq!(res_, 4);
    }
}
