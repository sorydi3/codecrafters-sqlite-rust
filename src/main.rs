use anyhow::{bail, Result};
use core::str;
use std::fs::File;
use std::io::prelude::*;

#[derive(Debug, Clone)]
enum PageType {
    LEAFINDEX,
    LEAFTABLE,
    INTERIORINDEX,
    INTERIORTABLE,
    UNKNOWNTYPE,
}

// return 2 bytes = u16 1 byte = u8

type PageSizeFields = u64;

const OFFSET: usize = 100;

#[derive(Debug, Clone)]
struct DatabaseHeader {
    header_str: String, // 16 bytes
    page_size: u16,     // 2 bytes offset 16
}

impl DatabaseHeader {
    fn new(database_header: &[u8]) -> Self {
        let header_str = str::from_utf8(&database_header[0..15])
            .expect("AN ISSUE HAPPENED")
            .to_string();
        let page_size: u16 = u16::from_be_bytes([database_header[16], database_header[17]]);
        Self {
            header_str,
            page_size,
        }
    }

    fn page_size(self) -> u16 {
        self.page_size
    }
}
#[derive(Debug, Clone)]
struct Page {
    type_page: PageType,
    table_count: u16,       // two bytes
    cell_content_area: u16, // two bytes
}

impl Page {
    fn new(database_page: &[u8]) -> Self {
        Self {
            type_page: Page::get_page_type(database_page[0]),
            table_count: u16::from_be_bytes([database_page[3], database_page[4]]),
            cell_content_area: u16::from_be_bytes([database_page[5], database_page[6]]),
        }
    }
    fn get_page_type(byte: u8) -> PageType {
        match byte {
            0x02 => PageType::INTERIORINDEX,
            0x05 => PageType::INTERIORTABLE,
            0x0A => PageType::LEAFINDEX,
            0x0D => PageType::LEAFTABLE,
            _ => PageType::UNKNOWNTYPE,
        }
    }

    fn get_table_count(self) -> u16 {
        self.table_count
    }
}

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
    match command.as_str() {
        ".dbinfo" => {
            let mut file = File::open(&args[1])?;

            let mut header = [0; 108];
            file.read_exact(&mut header)?;
            //let Header = DatabaseHeader::new(header);

            // The page size is stored at the 16th byte offset, using 2 bytes in big-endian order
            // Uncomment this block to pass the first stage
            println!(
                "database page size: {}",
                DatabaseHeader::new(&header[0..=99]).page_size()
            );
            println!(
                "number of tables: {}",
                Page::new(&header[OFFSET..OFFSET + 8]).get_table_count()
            );
        }
        _ => bail!("Missing or invalid command passed: {}", command),
    }

    Ok(())
}
