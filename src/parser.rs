use regex::Regex;

pub fn parse_sql_(sql: String) -> Option<(Vec<String>, String, Option<String>)> {
    // Regex to capture columns and table name
    let re =
        Regex::new(r"(?i)select\s+(?P<columns>[\w\s,\(\)\*]+)\s+from\s+(?P<table>\w+)(?:\s+where\s+(?P<condition>[\w\s=\']+))?").unwrap();

    if let Some(caps) = re.captures(&sql) {
        let columns = caps.name("columns").unwrap().as_str();
        let table = caps.name("table").unwrap().as_str();

        let condition = caps.name("condition").map(|c| c.as_str().to_string());

        // Split columns by comma and trim whitespace
        let columns: Vec<String> = columns.split(',').map(|s| s.trim().to_string()).collect();

        Some((columns, String::from(table), condition))
    } else {
        None
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parser() {
        let sql_apples: String = "SELECT COUNT(*) FROM apples;".into();
        let sql_oranges: String = "SELECT COUNT(*) FROM oranges;".into();

        let (columns_apples, table_name_apples, _) =
            parse_sql_(sql_apples).expect("fn parser():SQL PARSE FAILED");

        assert_eq!(columns_apples, vec!["COUNT(*)".to_string()]);
        assert_eq!(table_name_apples, "apples");
        let (columns_oranges, table_name_oranges, _) =
            parse_sql_(sql_oranges.to_string()).expect("PARSE FAILED");
        assert_eq!(columns_oranges, vec!["COUNT(*)".to_string()]);
        assert_eq!(table_name_oranges, "oranges");
    }

    #[test]
    fn test_parser_sql_superhoreos() {
        let db_name: String = "superheroes".into();
        let sql_ = format!("SELECT COUNT(*) FROM {db_name};");

        // Test parsing
        let (columns, table, _) =
            parse_sql_(sql_.clone()).expect("Failed to parse superheroes query");
        assert_eq!(columns, vec!["COUNT(*)".to_string()]);
        assert_eq!(table, db_name);
    }

    #[test]
    fn test_parser_sql_companies() {
        let db_name: String = "companies".into();
        let sql_ = format!("SELECT COUNT(*) FROM {db_name};");

        // Test parsing
        let (columns, table, _) =
            parse_sql_(sql_.clone()).expect("Failed to parse companies query");
        assert_eq!(columns, vec!["COUNT(*)".to_string()]);
        assert_eq!(table, db_name);
    }

    #[test]
    fn test_parser_sql_() {
        // Test cases with different SQL queries
        let test_cases = vec![
            (
                "SELECT name,age,city FROM users;",
                vec!["name", "age", "city"],
                "users",
            ),
            (
                "SELECT id, name FROM employees;",
                vec!["id", "name"],
                "employees",
            ),
            ("SELECT * FROM customers;", vec!["*"], "customers"),
        ];

        for (sql, expected_columns, expected_table) in test_cases {
            if let Some((columns, table, _)) = parse_sql_(sql.to_string()) {
                assert_eq!(columns, expected_columns);
                assert_eq!(table, expected_table);
            } else {
                panic!("Failed to parse SQL query: {}", sql);
            }
        }
    }

    #[test]
    fn test_parser_with_where() {
        let sql_apples: String = "SELECT COUNT(*) FROM apples;".into();

        let (columns_apples, table_name_apples, condition) =
            parse_sql_(sql_apples).expect("fn parser():SQL PARSE FAILED");

        assert_eq!(columns_apples, vec!["COUNT(*)".to_string()]);
        assert_eq!(table_name_apples, "apples");
        assert_eq!(condition, None);

        // Test with WHERE clause
        let sql_with_where = "SELECT name FROM users WHERE id = 5;".into();
        let (columns, table, condition) =
            parse_sql_(sql_with_where).expect("Failed to parse WHERE clause");

        assert_eq!(columns, vec!["name".to_string()]);
        assert_eq!(table, "users");
        assert_eq!(condition, Some("id = 5".to_string()));
    }
}
