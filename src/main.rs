use std::sync::Arc;

use anyhow::{bail, Result};
//use header::DatabaseHeader;
mod db;
use db::db::Db;
mod parser;
use parser::parse_sql;

fn main() -> Result<()> {
    // Parse arguments
    let args = std::env::args().collect::<Vec<_>>();
    match args.len() {
        0 | 1 => bail!("Missing <database path> and <command>"),
        2 => bail!("Missing <command>"),
        _ => {}
    }

    // Parse command and act accordingly
    let command = &args[2];

    let db: Arc<Db> = Arc::new(Db::new(args[1].clone()));

    match command.as_str() {
        ".dbinfo" => {
            println!("database page size: {:?}", db.get_page_size());

            println!("number of tables: {}", db.get_table_count_schema_page());
        }
        ".tables" => {
            db.get_schema_page().display_cells();
        }
        _ => {
            println!(
                "{:?}",
                parse_sql(command.to_string(), db.clone()).expect("PARSE FAILED")
            );
        }
    }
    Ok(())
}
