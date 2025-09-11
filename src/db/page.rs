use crate::db::header::HEADER_BYTES_SIZE;
use std::cell::RefCell;
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

type Rows = HashMap<String, Arc<(usize, RefCell<Page>)>>;

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct Page {
    offset: usize,
    type_page: PageType,
    table_count: u16,       // two bytes
    cell_content_area: u16, // two bytess
    rows: Rows,
    sql_schema: String,
}

impl Page {
    pub fn new_(
        file: &mut Arc<File>,
        page_number: usize,
        page_size: usize,
        sql_schema: String,
    ) -> Self {
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
            rows: HashMap::default(),
            sql_schema: sql_schema,
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
            rows: HashMap::default(), // rows
            sql_schema: String::default(),
        }
        .fill_cell_vec(file, page_size)
    }

    #[allow(dead_code)]
    pub fn get_rows_colum_names(&self, _table_name: String) -> Vec<Vec<String>> {
        match self.sql_schema.eq("") {
            true => {
                let page = self
                    .rows
                    .get(&_table_name)
                    .expect("Page::get_rows_colum_names():GET TABLE NAME FAILED!!");
                page.1.borrow().get_rows_colum_names(_table_name)
            }
            _ => self
                .sql_schema
                .split("(")
                .map(|c| c.to_string())
                .last()
                .unwrap()
                .replace(")", "")
                .replace("\n", "")
                .replace("\t", "")
                .trim()
                .split(",")
                .map(|s| s.trim().to_string())
                .map(|s| {
                    s.split(" ")
                        .map(|s| s.trim().to_string())
                        .collect::<Vec<String>>()
                })
                .collect::<Vec<Vec<String>>>(),
        }
        // THIS ONLY WORK FOR NON SCHEMA PAGES
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

    fn add_page(&mut self, file: &mut Arc<File>, row_offset: u16, page_size: usize) -> () {
        let (table_name, table_number, sql_schema) = self
            .get_cell_value_schema_page(file, row_offset)
            .expect("FAILED TO READ CELL VALUE");

        self.rows.entry(table_name).or_insert(Arc::new((
            table_number,
            RefCell::new(Page::new_(file, table_number, page_size, sql_schema)),
        )));
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

                    self.add_page(file, row_offset, page_size);
                    // add the cells vector
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
        let _res = self
            .rows
            .iter()
            .filter_map(|c| match **c.0 != "sqlite_sequence".to_string() {
                true => Some(c.clone().0.clone()),
                _ => None,
            })
            .collect::<Vec<String>>();

        println!("{:?}", _res.join(" "));
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

        Some((res, value as usize))
    }

    fn get_size_from_varint(&self, serialtype: u8) -> (RecordFieldType, usize) {
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
        match self.rows.get::<String>(&table_name) {
            Some(res) => res.1.borrow().get_cell_count_page(),
            _ => None,
        }
    }
    fn get_cell_value_schema_page(
        // get the table_name_schema
        &mut self,
        file: &mut Arc<File>,
        cell_offset: u16,
    ) -> Result<(String, usize, String), anyhow::Error> {
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

        let mut size = self.get_size_from_varint(display_values().0 as u8).1;
        size += self.get_size_from_varint(display_values().0 as u8).1;

        let (_, size_schema_table_name) = self.get_size_from_varint(display_values().0 as u8);

        let (_, size_rootpage) = self.get_size_from_varint(display_values().0 as u8);

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
            self.get_size_from_varint(schema_sql_size as u8).1,
        );
        let res: (String, usize, String) = (res, page_number as usize, sql_statement);
        Ok(res)
    }

    fn filter_columns(
        &self,
        columns: &[&str],
        table: Vec<Vec<Vec<(String, String)>>>,
    ) -> Vec<Vec<String>> {
        table[0]
            .iter()
            .map(|row| {
                row.iter()
                    .filter(|col| columns.as_ref().contains(&&col.0.as_str()))
                    .collect::<Vec<_>>()
            })
            .map(|row| row.iter().map(|col| col.1.clone()).collect::<Vec<String>>())
            .collect::<Vec<Vec<String>>>()
    }

    pub fn display_table_colums(
        &mut self,
        file: &mut Arc<File>,
        columns: &[&str],
        table_name: String,
    ) -> Vec<Vec<String>> {
        let rows: Vec<Vec<Vec<(String, String)>>> = self
            .rows
            .iter_mut()
            .filter(|table| (*(*table).0).eq(&table_name))
            .map(|c| {
                let (table_name, table_data) = c;
                table_data
                    .1
                    .borrow_mut()
                    .parse_page(table_name.clone(), file)
            })
            .collect();
        let resp: Vec<Vec<String>> = self.filter_columns(columns, rows);
        resp
    }

    pub fn parse_row_data(
        &self,
        row_offset: u64,
        table_name: String,
        file: &mut Arc<File>,
    ) -> Vec<(String, String)> {
        let page_offset = self.offset as u64;

        let row_offset_relative_current_page = (page_offset + row_offset) as usize;
        let row_size = self.decode_var_int(row_offset_relative_current_page, file);

        let row_id_offeset = row_offset_relative_current_page + row_size.as_ref().unwrap().0.len(); // offset row id
        let row_id = self.decode_var_int(row_id_offeset, file);

        let offset_size_header_byte_array =
            row_id_offeset + row_id.as_ref().unwrap().0.iter().len();
        let size_header_byte_array = self
            .decode_var_int(offset_size_header_byte_array, file)
            .as_ref()
            .unwrap()
            .1;
        let mut buffer_header_byter_array_cell_serial_types = vec![0; size_header_byte_array];

        file.seek(std::io::SeekFrom::Start(
            offset_size_header_byte_array as u64,
        ))
        .expect(format!("Failed to seek to offset{row_offset:?}").as_str());

        file.read_exact(&mut buffer_header_byter_array_cell_serial_types)
            .expect("READ EXACT FAILED!!");

        let sizes_fields = buffer_header_byter_array_cell_serial_types
            .iter()
            .skip(1)
            .map(|value| self.get_size_from_varint(*value).1)
            .collect::<Vec<_>>();
        // heady + data =  row_size
        assert!(
            sizes_fields.iter().sum::<usize>() + buffer_header_byter_array_cell_serial_types.len()
                == row_size.as_ref().unwrap().1
        ); // check that the header size and the sum of fields is equal to the row size.

        // seek to offset data
        let data_offset_byte_array =
            offset_size_header_byte_array + buffer_header_byter_array_cell_serial_types.len();
        file.seek(std::io::SeekFrom::Start(data_offset_byte_array as u64))
            .expect("seek failed");

        let mut offset = data_offset_byte_array;

        let row_data: Vec<String> = sizes_fields
            .iter()
            .map(|size| {
                //
                let res = self.read_bytes_to_utf8(file, offset, *size);
                //res = res.replace(" ", row_id.as_ref().unwrap().1.to_string().as_str());

                offset += *size;

                match res.eq("") {
                    true => row_id.as_ref().unwrap().1.to_string(),
                    _ => res,
                }
            })
            .collect();
        let column_names = self.get_rows_colum_names(table_name.clone());
        assert!(row_data.len() == column_names.len()); // assert colum data length and colum types are equal
                                                       // join colum names and data of the current row
        let res = column_names
            .iter()
            .zip(row_data.iter())
            .map(|c| (c.0[0].clone(), c.1.clone()))
            .collect::<Vec<_>>();

        res
    }

    fn parse_page(
        &mut self,
        table_name: String,
        file: &mut Arc<File>,
    ) -> Vec<Vec<(String, String)>> {
        let offset_page_header = 8;
        file.seek(std::io::SeekFrom::Start(
            (self.offset + offset_page_header) as u64,
        ))
        .expect("SEEK FAILED!!"); // seek to offset page

        //TODO -> CHECK FOR PAGE TYPE FOR NON LEAF PAGES THERE MIGHT BE RIGHT POINTER BEFORE THE CELLS START.
        // USE A MATCH TO CHECK THE TYPE OF THE PAGE

        let size_cell_pointer = 2;
        let mut buffer = vec![0u8; (self.table_count * size_cell_pointer) as usize];
        file.read_exact(&mut buffer).expect("READ EXACT FAILED!!");
        //seek from the start of the page
        let res: Vec<Vec<(String, String)>> = buffer
            .chunks(2) // cell size
            .map(|cell| u16::from_be_bytes([cell[0], cell[1]]))
            .map(|offeset_cell| self.parse_row_data(offeset_cell as u64, table_name.clone(), file))
            .collect();
        res
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
        let schema = &db.get_schema_page();
        let cells_schema_page = &schema.borrow().rows;
        assert!(cells_schema_page.len() > 0);
    }

    #[test]
    fn test_cells_content() {
        let res: &str = "apples sqlite_sequence oranges";
        let db = get_db_instance();
        let schema = &db.get_schema_page();
        let cells_schema_page = &schema.borrow().rows;
        assert!(cells_schema_page.len() == 3);
        assert!(cells_schema_page.iter().all(|c| res.contains(c.0)));
    }

    #[test]
    fn test_schema_test_page_type() {
        let res_page_type: PageType = PageType::LEAFTABLE;
        let db = get_db_instance();
        let schema = &db.get_schema_page();
        let page_type: &PageType = &schema.borrow().type_page;
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
        let rows = vec![("oranges", 6), ("apples", 4)];
        rows.iter().for_each(|table| {
            if let Some(count_cells) = &db.get_schema_page().borrow().rows.get(table.0) {
                let count = count_cells
                    .1
                    .borrow()
                    .get_cell_count_page()
                    .expect("COUNT PAGE FAILED");
                assert_eq!(count, table.1)
            } else {
                assert!(false)
            }
        })
    }

    #[test]
    fn get_column_names_db_sample() {
        let db = get_db_instance();

        let table_name: String = "oranges".into();

        let res: Vec<Vec<String>> = [
            [
                "id".into(),
                "integer".into(),
                "primary".into(),
                "key".into(),
                "autoincrement".into(),
            ]
            .to_vec(),
            ["name".into(), "text".into()].to_vec(),
            ["description".into(), "text".into()].to_vec(),
        ]
        .to_vec();

        let col_names_db = db
            .get_schema_page()
            .borrow()
            .rows
            .get("oranges")
            .unwrap()
            .1
            .borrow()
            .get_rows_colum_names(table_name);
        println!("{:?}", col_names_db);

        assert!(col_names_db.iter().eq(res.iter()));
    }

    #[test]
    fn test_decode_varint() {
        let offset_oranges_cell_1_size_record = 3779;
        let offset_oranges_cell_sql_squema = 3786;
        let db = get_db_instance();
        let mut file = db.get_file();
        let schema_page = db.get_schema_page();
        let (bytes, value) = schema_page
            .borrow()
            .decode_var_int(offset_oranges_cell_1_size_record, &mut file)
            .expect("DECODE VARINT FAILED");
        assert_eq!(value, 120);
        assert_eq!(bytes.len(), 1);

        let (bytes, value) = schema_page
            .borrow()
            .decode_var_int(offset_oranges_cell_sql_squema, &mut file)
            .expect("DECODE VARINT FAILED");

        assert_eq!(value, 199);
        assert_eq!(bytes.len(), 2);
    }

    #[test]
    fn test_get_size_from_varint() {
        let offset_oranges_cell_sql_squema = 3786;
        let db = get_db_instance();
        let mut file = db.get_file();
        let schema_page = db.get_schema_page();
        let size = schema_page.borrow().get_size_from_varint(
            schema_page
                .borrow()
                .decode_var_int(offset_oranges_cell_sql_squema, &mut file)
                .expect("DECODE VARINT FAILED")
                .1 as u8,
        );
        assert_eq!(size.1, 93);
    }

    #[test]
    fn test_display_columns_given_2_columns() {
        let db = get_db_instance();
        let schema_page = db.get_schema_page();
        let mut file: Arc<File> = db.get_file();
        let table_name = "oranges";
        let expected_rows = vec![
            vec!["Mandarin".to_string(), "great for snacking".to_string()],
            vec!["Tangelo".to_string(), "sweet and tart".to_string()],
            vec![
                "Tangerine".to_string(),
                "great for sweeter juice".to_string(),
            ],
            vec![
                "Clementine".to_string(),
                "usually seedless, great for snacking".to_string(),
            ],
            vec![
                "Valencia Orange".to_string(),
                "best for juicing".to_string(),
            ],
            vec![
                "Navel Orange".to_string(),
                "sweet with slight bitterness".to_string(),
            ],
        ];
        let actual_rows = schema_page.borrow_mut().display_table_colums(
            &mut file,
            &vec!["name", "description"],
            table_name.to_string(),
        );

        let mut actual_sorted = actual_rows.clone();
        let mut expected_sorted = expected_rows.clone();
        actual_sorted.sort();
        expected_sorted.sort();
        assert_eq!(actual_sorted, expected_sorted);
    }

    #[test]
    fn test_display_columns_given_1_column() {
        let db = get_db_instance();
        let schema_page = db.get_schema_page();
        let mut file: Arc<File> = db.get_file();
        let table_name = "oranges";

        let expected_rows = vec![
            vec!["Mandarin".to_string()],
            vec!["Tangelo".to_string()],
            vec!["Tangerine".to_string()],
            vec!["Clementine".to_string()],
            vec!["Valencia Orange".to_string()],
            vec!["Navel Orange".to_string()],
        ];

        let actual_rows = schema_page.borrow_mut().display_table_colums(
            &mut file,
            &vec!["name"],
            table_name.to_string(),
        );

        let mut actual_sorted = actual_rows.clone();
        let mut expected_sorted = expected_rows.clone();
        actual_sorted.sort();
        expected_sorted.sort();
        assert_eq!(actual_sorted, expected_sorted);
    }
}
