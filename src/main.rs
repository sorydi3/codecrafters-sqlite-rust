use std::sync::Arc;

use anyhow::{bail, Ok, Result};
use std::result::Result::Ok as OK;

mod db;
use db::db::Db;
mod parser;
use parser::parse_sql_;

use crate::db::page::Page;
use std::cell::RefCell;

fn search_by_index(condition: &Option<String>) -> Option<(bool, String)> {
    match condition {
        Some(cond) => {
            let vec = cond
                .split("=")
                .map(|s| s.trim().replace("'", ""))
                .collect::<Vec<_>>();

            let col_name = vec.get(0).unwrap();
            let col_value = vec.get(1).unwrap();

            Some((col_name.eq("country"), col_value.clone()))
        }
        _ => None,
    }
}

fn get_column_data(
    table_name: String,
    columns: Vec<String>,
    schema_page: Arc<RefCell<Page>>,
    db: &mut Arc<Db>,
    condition: &Option<String>,
) -> Result<String> {
    let columns_refs: Vec<&str> = columns.iter().map(AsRef::as_ref).collect();
    let columns_names = match search_by_index(condition) {
        Some((true, key)) => {
            let res = schema_page.borrow_mut().search_index_country(
                &mut db.get_file(),
                (table_name, "idx_companies_country".into()),
                key,
            );
            vec![res]
        }
        _ =>{
            schema_page
            .borrow_mut()
            .get_table_data(&mut db.get_file(), table_name.clone())
        } 
    };

    let res = columns_names[0]
        .iter()
        .filter(|row| match condition {
            // filter for where clause
            Some(cond) => {
                // only when where clause is available
                let cond = cond
                    .split("=")
                    .map(|s| s.trim().replace("'", ""))
                    .collect::<Vec<_>>();

                let col_name = cond.get(0).unwrap();
                let col_value = cond.get(1).unwrap();

                let (index, colum_name) = row
                    .iter()
                    .enumerate()
                    .find(|c| c.1 .0.eq(col_name))
                    .unwrap();
                assert!(colum_name.0.eq(col_name));
                row[index].1.eq(col_value)
            }
            _ => true,
        })
        .map(|row| row.clone())
        .collect::<Vec<_>>();

    
    let filtered = Page::filter_columns(&columns_refs.as_ref(), res);
    let resp = filtered.iter().map(|c| c.join("|")).collect::<Vec<_>>();
    let out = resp.join("\n");
    Ok(out)
}

fn handle_sql_query(sql_query: String, db: &mut Arc<Db>) -> Result<String> {
    let is_select = sql_query.to_ascii_lowercase().starts_with("select");
    assert!(is_select); // must be a select
    let (columns, table_name, condition) = parse_sql_(sql_query).expect("FAILED TO PARSE SQL");
    let count: String = "count(*)".into();

    let schema_page = db.get_schema_page();
    match columns.join("").to_ascii_lowercase().contains(&count) {
        true => {
            let res = schema_page.borrow().get_cell_count_page_schema(table_name);
            Ok(res.unwrap().to_string())
        }
        _ => match columns.iter().any(|c| c.eq("*")) {
            true => {
                let columns: Vec<String> = schema_page
                    .borrow()
                    .get_rows_colum_names(table_name.clone(), false)
                    .iter()
                    .map(|c| {
                        let col = c[0].clone();
                        col
                    })
                    .collect();

                get_column_data(table_name, columns, schema_page, db, &condition)
                // display all columns
            }
            _ => {
                let res = get_column_data(table_name, columns, schema_page, db, &condition);
                res
            } // just for some columns
        },
    }
}

fn main() -> Result<()> {
    //Parse arguments
    let args = std::env::args().collect::<Vec<_>>();
    match args.len() {
        0 | 1 => bail!("Missing <database path> and <command>"),
        2 => bail!("Missing <command>"),
        _ => {}
    }

    // Parse command and act accordingly
    let command = &args[2];

    let mut db: Arc<Db> = Arc::new(Db::new(args[1].clone()));

    match command.as_str() {
        ".dbinfo" => {
            println!("database page size: {:?}", db.get_page_size());

            println!("number of tables: {}", db.get_table_count_schema_page());
        }
        ".tables" => {
            db.get_schema_page().borrow().display_cells();
        }
        _ => {
            let res = handle_sql_query(command.to_string(), &mut db).unwrap();

            match res.parse::<usize>() {
                OK(count) => {
                    println!("{:?}", count);
                }
                Err(_) => {
                    // Simplified printing without quotes
                    for row in res.split('\n') {
                        println!("{}", row);
                    }
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_handle_query() {
        // Setup
        let mut db = Arc::new(Db::new("sample.db".to_string()));

        // Test COUNT(*) query
        let count_query = "SELECT COUNT(*) FROM oranges;".to_string();
        let count_result =
            handle_sql_query(count_query, &mut db).expect("Failed to handle COUNT query");
        assert_eq!(count_result, "6");

        // Test SELECT * query
        let select_query = "SELECT * FROM oranges;".to_string();
        let select_result =
            handle_sql_query(select_query, &mut db).expect("Failed to handle SELECT query");

        // Split result into lines and verify content
        let rows: Vec<&str> = select_result.split('\n').collect();
        assert!(rows.len() == 6)
    }
}
