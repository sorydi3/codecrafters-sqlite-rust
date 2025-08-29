use crate::db::header::HEADER_BYTES_SIZE;
use std::collections::HashMap;
use std::fs::File;
use std::io::prelude::*;
use std::sync::Arc;

const OFFSET: usize = 100;
#[derive(Debug, Copy, Clone, Default)]
#[allow(dead_code)]
enum RecordFieldType {
    #[default]
    Null,
    I8,
    I16,
    I24,
    I32,
    I48,
    I64,
    Float,
    Zero,
    One,
    STRING(usize),
    BLOB(usize),
}

#[derive(Debug, Clone, PartialEq)]
enum PageType {
    LEAFINDEX,
    LEAFTABLE,
    INTERIORINDEX,
    INTERIORTABLE,
    UNKNOWNTYPE,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]

pub struct Page {
    offset: usize,
    type_page: PageType,
    table_count: u16,       // two bytes
    cell_content_area: u16, // two bytess
    cells: HashMap<String, Arc<(usize, Box<Page>)>>,
}

impl Page {
    pub fn new_(file: &mut Arc<File>, page_number: usize, page_size: usize) -> Self {
        let mut database_page = vec![0; page_size];
        let page_offset = Page::get_offset_page(page_number, page_size);
        file.seek(std::io::SeekFrom::Start(page_offset as u64))
            .expect("SEEK_FILED!!");
        file.read_exact(&mut database_page).expect("READ FAILED!!");

        Self {
            offset: Page::get_offset_page(page_number, page_size), // offset default to schema page
            type_page: Page::get_page_type(database_page[0]),
            table_count: u16::from_be_bytes([database_page[3], database_page[4]]),
            cell_content_area: u16::from_be_bytes([database_page[5], database_page[6]]),
            cells: HashMap::default(),
        }
    }

    pub fn new__(file: &mut Arc<File>, page_number: usize, page_size: usize) -> Self {
        assert!(page_number >= 1);
        let offset_page = Page::get_offset_page(page_number, page_size);
        file.seek(std::io::SeekFrom::Start(offset_page as u64))
            .expect("SEEK FAILED!!");
        let mut database_page = vec![0u8; page_size as usize];
        file.read_exact(&mut database_page[..])
            .expect("BUFFER READ FAILED!!");

        Self {
            offset: offset_page,
            type_page: Page::get_page_type(database_page[0]),
            table_count: u16::from_be_bytes([database_page[3], database_page[4]]),
            cell_content_area: u16::from_be_bytes([database_page[5], database_page[6]]),
            cells: HashMap::default(),
        }
        .fill_cell_vec(file, page_size)
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
    pub fn fill_cell_vec(mut self, file: &mut Arc<File>, page_size: usize) -> Self {
        let schema_header_offeset = 8;
        let mut init_offset = OFFSET + schema_header_offeset;
        let res = file.seek(std::io::SeekFrom::Start(init_offset as u64));
        match res {
            Ok(_res) => {
                let mut i = 1;
                let step = 2;

                while i <= self.table_count {
                    // iterate throught the cells of the schema table
                    let mut buffer: [u8; 2] = [0; 2];
                    file.read_exact(&mut buffer).expect("READ EXACT FAILED!!");

                    let row_offset = u16::from_be_bytes([buffer[0], buffer[1]]);

                    let res = self
                        .get_cell_value_schema_page(file, row_offset)
                        .expect("FAILED TO READ CELL VALUE");

                    self.cells.entry(res.0).or_insert(Arc::new((
                        res.1,
                        Box::new(Page::new_(file, res.1, page_size)),
                    ))); // add the cells vector
                         //seek to the row data using the the current
                         //let offse_row_data = file.seek_relative(offset)
                    init_offset = init_offset + step;

                    file.seek(std::io::SeekFrom::Start(init_offset as u64))
                        .expect("SEEK FAILED!!"); //

                    i += 1;
                }
            }
            Err(x) => {
                println!("SOMTHING WENT WRONG!! {}", x)
            }
        }

        self
    }

    pub fn display_cells(&self) {
        let res = self
            .cells
            .iter()
            .filter_map(|c| match **c.0 != "sqlite_sequence".to_string() {
                true => Some(c.clone().0.clone()),
                _ => None,
            })
            .collect::<Vec<String>>();

        println!("{:?}", res.join(" "));
    }

    pub fn get_table_count(&self) -> u16 {
        self.table_count
    }

    #[allow(dead_code)]
    fn get_varint_buffer(file: &mut File, offset: usize, size: usize) -> Vec<u8> {
        let mut buffer = vec![0; size];
        let _ = file.seek(std::io::SeekFrom::Start(offset as u64));
        file.read_exact(&mut buffer).expect("READ_FAILED");
        buffer
    }

    fn decode_var_int(&self, offset: usize, file: &mut Arc<File>) -> Option<(Vec<u8>, usize)> {
        file.seek(std::io::SeekFrom::Start(offset as u64))
            .expect("SEEK FAILED!!");
        let mut payload = vec![0; 9];
        file.take(9)
            .read(&mut payload)
            .expect("READ FILED VARINT BUFFER");
        let mut res = vec![];
        let mut it = 0;
        let mut value: u64 = 0;
        let mut found = false;
        while !found && it < 9 {
            res.push(payload[it]);
            // construct the final value by joining bits
            let data_bits = (payload[it] & 0b0111_1111) as u64;
            value = (value << 7) | data_bits;
            // test if last bit is set
            if !(payload[it] & 0b1000_0000 == 0b1000_0000) {
                // test if last bit is set
                found = true
            }
            it += 1;
        }

        //println!(" VALUE DECODE : {value:?}");
        Some((res, value as usize))
    }

    fn parse_record_header(&self, serialtype: u8) -> (RecordFieldType, usize) {
        let (field_type, field_size) = match serialtype {
            0 => (RecordFieldType::Null, 0),
            1 => (RecordFieldType::I8, 1),
            2 => (RecordFieldType::I16, 2),
            3 => (RecordFieldType::I24, 3),
            4 => (RecordFieldType::I32, 4),
            5 => (RecordFieldType::I48, 6),
            6 => (RecordFieldType::I64, 8),
            7 => (RecordFieldType::Float, 8),
            8 => (RecordFieldType::Zero, 0),
            9 => (RecordFieldType::One, 0),
            n if n >= 12 && n % 2 == 0 => {
                let size = ((n - 12) / 2) as usize;
                (RecordFieldType::BLOB(size), size)
            }
            n if n >= 13 && n % 2 == 1 => {
                let size = ((n - 13) / 2) as usize;
                (RecordFieldType::STRING(size), size)
            }
            _ => panic!("NOT SUPORTED TYPE"),
        };

        (field_type, field_size)
    }

    fn read_bytes_to_utf8(&self, file: &mut Arc<File>, offset: usize, size: usize) -> String {
        //println!("{:x?}",&self.payload[offset..offset+size]);
        let mut buff = vec![0; size];
        file.seek(std::io::SeekFrom::Start(offset as u64))
            .expect("SEEK read_bytes() failed");

        file.read_exact(&mut buff)
            .expect("read_exact() from read_bytes() failed ");

        let res = match buff.len() {
            1 => u8::from_be(buff[0]).to_string(),
            2 => u16::from_be_bytes([buff[0], buff[1]]).to_string(),
            _ => String::from_utf8_lossy(&buff).to_string(),
        };
        res
    }

    pub fn get_offset_page(page_number: usize, page_size: usize) -> usize {
        let offset_page = match page_number - 1 {
            0 => 0 + HEADER_BYTES_SIZE as usize,
            1 => page_size,
            _ => (page_number - 1) * page_size,
        };
        offset_page
    }
    #[allow(dead_code)]
    fn get_cell_count_page(&self) -> Option<usize> {
        //WARNING --> ONLY FOR SQUEMA PAGES
        Some(self.table_count as usize)
    }
    #[allow(dead_code)]
    pub fn get_cell_count_page_schema(&self, table_name: String) -> Option<usize> {
        //WARNING --> ONLY FOR SQUEMA PAGES
        match self.cells.get::<String>(&table_name) {
            Some(res) => res.1.get_cell_count_page(),
            _ => None,
        }
    }

    fn get_cell_value_schema_page(
        // get the table_name_schema
        &mut self,
        file: &mut Arc<File>,
        cell_offset: u16,
    ) -> Result<(String, usize), anyhow::Error> {
        let mut offset: usize = cell_offset as usize;

        let mut payload_size_value = 0;
        let mut display_values = || {
            if let Some((bytes, value)) = self.decode_var_int(offset as usize, file) {
                payload_size_value = value;
                offset += bytes.len();
                (payload_size_value, offset)
            } else {
                (0, 0)
            }
        };

        display_values(); // Size of the record (varint)
        display_values(); //The rowid (safe to ignore)
        display_values(); // Size of record header (varint

        let mut size = self.parse_record_header(display_values().0 as u8).1;
        size += self.parse_record_header(display_values().0 as u8).1;

        let (_, size_schema_table_name) = self.parse_record_header(display_values().0 as u8);

        let (_, size_rootpage) = self.parse_record_header(display_values().0 as u8);

        let (schema_sql_size, schema_sql_offset) = display_values();
        let tbl_name_offset = schema_sql_offset + size;

        let res = self.read_bytes_to_utf8(file, tbl_name_offset, size_schema_table_name);

        let offset_rootpage_pagenumber = tbl_name_offset + size_schema_table_name;

        let page_number = self
            .read_bytes_to_utf8(file, offset_rootpage_pagenumber, size_rootpage)
            .parse::<u16>()
            .unwrap();

        let offset_sql_sqlite_schema = offset_rootpage_pagenumber + size_rootpage;
        let sql_statement = self.read_bytes_to_utf8(
            file,
            offset_sql_sqlite_schema,
            self.parse_record_header(schema_sql_size as u8).1,
        );
        println!("SQL-STATEMENT: {sql_statement:?}");
        let res: (String, usize) = (res, page_number as usize);
        Ok(res)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::db::Db;

    fn get_db_instance() -> Db {
        let db_file_path: String =
            "/Users/ollaid/codecrafters/codecrafters-sqlite-rust/sample.db".into();
        let db = Db::new(db_file_path.clone());
        db
    }

    #[test]
    fn test_table_count_schema_page() {
        let db = get_db_instance();
        let page_schema_table_count = db.get_table_count_schema_page();
        assert_eq!(page_schema_table_count, 3);
    }

    #[test]
    fn test_cells_filled_schema_page() {
        let db = get_db_instance();
        let cells_schema_page = &db.get_schema_page().cells;
        assert!(cells_schema_page.len() > 0);
    }

    #[test]
    fn test_cells_content() {
        let res: &str = "apples sqlite_sequence oranges";
        let db = get_db_instance();
        let cells_schema_page = &db.get_schema_page().cells;
        assert!(cells_schema_page.len() == 3);
        assert!(cells_schema_page.iter().all(|c| res.contains(c.0)));
    }

    #[test]
    fn test_schema_test_page_type() {
        let res_page_type: PageType = PageType::LEAFTABLE;
        let db = get_db_instance();
        let page_type: &PageType = &db.get_schema_page().type_page;
        assert!(*page_type == res_page_type);
    }

    #[test]
    fn test_get_page_offset_() {
        let db = get_db_instance();
        assert!(Page::get_offset_page(1, db.get_page_size()) == 100);
        assert!(Page::get_offset_page(2, db.get_page_size()) == 4096);
        assert!(Page::get_offset_page(3, db.get_page_size()) == 8192);
        assert!(Page::get_offset_page(4, db.get_page_size()) == 12288);
    }

    #[test]
    fn test_cells_counts_for_squema_pages() {
        let db = get_db_instance();
        let tables = vec![("oranges", 6), ("apples", 4)];
        tables.iter().for_each(|table| {
            if let Some(count_cells) = &db.get_schema_page().cells.get(table.0) {
                let count = count_cells
                    .1
                    .get_cell_count_page()
                    .expect("COUNT PAGE FAILED");
                assert_eq!(count, table.1)
            } else {
                assert!(false)
            }
        })
    }
}
