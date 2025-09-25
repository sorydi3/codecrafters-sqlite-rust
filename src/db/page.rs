use crate::db::header::HEADER_BYTES_SIZE;
use core::panic;
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
    pub fn get_rows_colum_names(&self, _table_name: String, schema: bool) -> Vec<Vec<String>> {
        match self.sql_schema.eq("") {
            true => match schema {
                true => {
                    vec![
                        vec!["type".into(), "text".into()],
                        vec!["name".into(), "text".into()],
                        vec!["tbl_name".into(), "text".into()],
                        vec!["rootpage".into(), "integer".into()],
                        vec!["sql".into(), "text".into()],
                    ]
                }
                _ => {
                    let page = self
                        .rows
                        .get(&_table_name)
                        .expect("Page::get_rows_colum_names():GET TABLE NAME FAILED!!");
                    page.1.borrow().get_rows_colum_names(_table_name, false)
                }
            },
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
        let row_data = self.parse_row_data(row_offset as u64, " ".into(), file, true);
        let table_number = &row_data
            .iter()
            .find(|col| col.0.eq("rootpage"))
            .unwrap()
            .1
            .parse::<usize>()
            .unwrap();

        let table_name = row_data
            .iter()
            .find(|col| col.0.eq("name"))
            .unwrap()
            .1
            .clone();

        let sql = row_data
            .iter()
            .find(|col| col.0.eq("sql"))
            .unwrap()
            .1
            .clone();
        //println!("row_data---->: {:?}", row_data);

        self.rows.entry(table_name).or_insert(Arc::new((
            *table_number,
            RefCell::new(Page::new_(file, *table_number, page_size, sql)),
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
        let mut _res = self
            .rows
            .iter()
            .filter_map(|c| match **c.0 != "sqlite_sequence".to_string() {
                true => Some(c.clone().0.clone()),
                _ => None,
            })
            .collect::<Vec<String>>();
        _res.sort();
        print!("{}", _res.join(" "));
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

    fn get_size_from_varint(&self, serialtype: usize) -> (RecordFieldType, usize) {
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

    pub fn order_row_columns(
        &self,
        columns: &[&str],
        row: Vec<&(String, String)>,
    ) -> Vec<(String, String)> {
        let mut response: Vec<(String, String)> = vec![];

        for column in columns {
            let aux = row.iter().find(|c| c.0.eq(column));
            match aux {
                Some(res) => {
                    response.push((*res).clone());
                }
                _ => (),
            }
        }
        response
    }

    fn filter_columns(
        &self,
        columns: &[&str],
        table: Vec<Vec<Vec<(String, String)>>>,
    ) -> Vec<Vec<String>> {
        table[0]
            .iter()
            .map(|row| {
                //return only columns especified in columns parameter
                let res = row
                    .iter()
                    .filter(|col| columns.as_ref().contains(&&col.0.as_str()))
                    .collect::<Vec<_>>();
                self.order_row_columns(columns, res)
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

    pub fn get_varints_byte_array(
        &self,
        file: &mut Arc<File>,
        byte_array_header_size: usize,
        row_offset: u64,
    ) -> Vec<usize> {
        file.seek(std::io::SeekFrom::Start(row_offset))
            .expect("FAILED TO SEEK!!");

        let mut buffer_header: Vec<u8> = vec![];

        file.read_exact(&mut buffer_header)
            .expect("failed to read!!");

        let mut current_bytes_reads = 0;
        let mut end = false;
        let mut offset: usize = row_offset as usize;
        let mut response: Vec<usize> = vec![];
        while !end {
            let res = self.decode_var_int(offset, file);
            response.push(res.clone().unwrap().1);
            current_bytes_reads += res.clone().unwrap().0.len();

            if current_bytes_reads < byte_array_header_size {
                offset += res.clone().unwrap().0.len();
                file.seek(std::io::SeekFrom::Start(offset as u64))
                    .expect("FAILED TO SEEK!!");
            } else {
                end = true;
            }
        }

        response
    }

    pub fn parse_row_data(
        &self,
        row_offset: u64,
        table_name: String,
        file: &mut Arc<File>,
        schema: bool,
    ) -> Vec<(String, String)> {
        let page_offset = match self.sql_schema.eq("") {
            true => 0,
            _ => self.offset as u64,
        };

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
        //let mut buffer_header_byter_array_cell_serial_types = vec![0; size_header_byte_array];

        file.seek(std::io::SeekFrom::Start(
            offset_size_header_byte_array as u64,
        ))
        .expect(format!("Failed to seek to offset{row_offset:?}").as_str());
        let resp = self.get_varints_byte_array(
            file,
            size_header_byte_array,
            offset_size_header_byte_array as u64,
        );

        let sizes_fields = resp
            .iter()
            .skip(1)
            .map(|value| self.get_size_from_varint(*value).1)
            .collect::<Vec<_>>();

        // heady + data =  row_size

        assert!(
            sizes_fields.iter().sum::<usize>() + size_header_byte_array
                == row_size.as_ref().unwrap().1
        ); // check that the header size and the sum of fields is equal to the row size.

        // seek to offset data
        let data_offset_byte_array = offset_size_header_byte_array + size_header_byte_array;
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

        let column_names = self.get_rows_colum_names(table_name.clone(), schema);

        assert!(row_data.len() == column_names.len()); // assert colum data length and colum types are equal

        /*
         println!(
            "ROW_SIZE {:?} ROW_ID {:?} HEADER_SIZE: {:?} BUFFER_S_TYPES: {:?} SIZE_FIEDLS: {:?}, ROW_DATA: {:?}",
            row_size,
            row_id,
            size_header_byte_array,
            resp,
            sizes_fields,
            row_data
        );
         */
        // join colum names and data of the current row
        let res = column_names
            .iter()
            .zip(row_data.iter())
            .map(|c| (c.0[0].clone(), c.1.clone()))
            .collect::<Vec<_>>();

        res
    }

    pub fn add_page_to_rows(
        &mut self,
        row_offset: u64,
        file: &mut Arc<File>,
    ) -> Vec<(String, String)> {
        //

        file.seek(std::io::SeekFrom::Start(self.offset as u64))
            .expect("seeking failed!!"); //seek to the start of the paage

        file.seek_relative(row_offset as i64)
            .expect("FAILED TO SEEK!!");

        let mut row_buffer = vec![0u8; 5];

        file.read_exact(&mut row_buffer)
            .expect("READ EXACT FAILED!!");

        let page_number =
            u32::from_be_bytes([row_buffer[0], row_buffer[1], row_buffer[2], row_buffer[3]]);

        let _row_id = u8::from_be_bytes([row_buffer[4]]);

        self.rows
            .entry(format!("{page_number}"))
            .or_insert(Arc::new((
                page_number as usize,
                RefCell::new(Page::new_(
                    file,
                    page_number as usize,
                    4096,
                    self.sql_schema.clone(),
                )),
            )));
        vec![("".into(), "".into())]
    }

    fn parse_page(
        &mut self,
        table_name: String,
        file: &mut Arc<File>,
    ) -> Vec<Vec<(String, String)>> {
        let offset_page_header = 8; // offset for leaf pages

        let size_cell_pointer = 2;
        match self.type_page {
            PageType::INTERIORINDEX => panic!("INTERIOR INDEX NOT IMPLEMENTED YET"),
            PageType::INTERIORTABLE => {
                //
                //root page

                let offset_page_header = 12;

                file.seek(std::io::SeekFrom::Start(
                    (self.offset + offset_page_header) as u64,
                ))
                .expect("SEEK FAILED!!");

                let mut buffer = vec![0u8; (self.table_count * size_cell_pointer) as usize];
                file.read_exact(&mut buffer).expect("READ EXACT FAILED!!");

                let _res: Vec<Vec<(String, String)>> = buffer
                    .chunks(2) // cell size
                    .map(|cell| u16::from_be_bytes([cell[0], cell[1]]))
                    .map(|offeset_cell| self.add_page_to_rows(offeset_cell as u64, file))
                    .collect();

                let res_ = self
                    .rows
                    .iter_mut()
                    .flat_map(|data| {
                        let (_, page) = data;
                        page.1.borrow_mut().parse_page(table_name.clone(), file)
                        //println!("table_data {:?}",data);
                    })
                    .collect::<Vec<_>>();
                res_
            }
            PageType::LEAFTABLE => {
                file.seek(std::io::SeekFrom::Start(
                    (self.offset + offset_page_header) as u64,
                ))
                .expect("SEEK FAILED!!");
                let mut buffer = vec![0u8; (self.table_count * size_cell_pointer) as usize];
                file.read_exact(&mut buffer).expect("READ EXACT FAILED!!");
                //seek from the start of the page
                let res: Vec<Vec<(String, String)>> = buffer
                    .chunks(2) // cell size
                    .map(|cell| u16::from_be_bytes([cell[0], cell[1]]))
                    .map(|offeset_cell| {
                        self.parse_row_data(offeset_cell as u64, table_name.clone(), file, false)
                    })
                    .collect();
                res
            }
            PageType::LEAFINDEX => panic!("LEAD INDEX NOT IMPLEMENTED YET"),
            PageType::UNKNOWNTYPE => panic!("unknow page type!! File might be corrupted!!"),
        }

        //TODO -> CHECK FOR PAGE TYPE FOR NON LEAF PAGES THERE MIGHT BE RIGHT POINTER BEFORE THE CELLS START.
        // USE A MATCH TO CHECK THE TYPE OF THE PAGE
    }
}

#[cfg(test)]
mod tests {
    use std::vec;

    use super::*;
    use crate::db::db::Db;

    fn get_db_instance(db_name: String) -> Db {
        let db_file_path: String =
            format!("/Users/ollaid/codecrafters/codecrafters-sqlite-rust/{db_name}.db");
        let db = Db::new(db_file_path.clone());
        db
    }

    #[test]
    fn test_table_count_schema_page() {
        let db = get_db_instance("sample".into());
        let page_schema_table_count = db.get_table_count_schema_page();
        assert_eq!(page_schema_table_count, 3);
    }

    #[test]
    fn test_cells_filled_schema_page() {
        let db = get_db_instance("sample".into());
        let schema = &db.get_schema_page();
        let cells_schema_page = &schema.borrow().rows;
        assert!(cells_schema_page.len() > 0);
    }

    #[test]
    fn test_cells_content() {
        let res: &str = "apples sqlite_sequence oranges";
        let db = get_db_instance("sample".into());
        let schema = &db.get_schema_page();
        let cells_schema_page = &schema.borrow().rows;
        assert!(cells_schema_page.len() == 3);
        assert!(cells_schema_page.iter().all(|c| res.contains(c.0)));
    }

    #[test]
    fn test_schema_test_page_type() {
        let res_page_type: PageType = PageType::LEAFTABLE;
        let db = get_db_instance("sample".into());
        let schema = &db.get_schema_page();
        let page_type: &PageType = &schema.borrow().type_page;
        assert!(*page_type == res_page_type);
    }

    #[test]
    fn test_get_page_offset_() {
        let db = get_db_instance("sample".into());
        assert!(Page::get_offset_page(1, db.get_page_size()) == 100);
        assert!(Page::get_offset_page(2, db.get_page_size()) == 4096);
        assert!(Page::get_offset_page(3, db.get_page_size()) == 8192);
        assert!(Page::get_offset_page(4, db.get_page_size()) == 12288);
    }

    #[test]
    fn test_cells_counts_for_squema_pages() {
        let db = get_db_instance("sample".into());
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
        let db = get_db_instance("sample".into());

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
            .get_rows_colum_names(table_name, false);
        println!("{:?}", col_names_db);

        assert!(col_names_db.iter().eq(res.iter()));
    }

    #[test]
    fn test_decode_varint() {
        let offset_oranges_cell_1_size_record = 3779;
        let offset_oranges_cell_sql_squema = 3786;
        let db = get_db_instance("sample".into());
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
        let db = get_db_instance("sample".into());
        let mut file = db.get_file();
        let schema_page = db.get_schema_page();
        let size = schema_page.borrow().get_size_from_varint(
            schema_page
                .borrow()
                .decode_var_int(offset_oranges_cell_sql_squema, &mut file)
                .expect("DECODE VARINT FAILED")
                .1,
        );
        assert_eq!(size.1, 93);
    }

    #[test]
    fn test_display_columns_given_2_columns() {
        let db = get_db_instance("sample".into());
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
        let db = get_db_instance("sample".into());
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

    #[test]
    fn test_order_columns() {
        let db = get_db_instance("sample".into());
        let schema_page = db.get_schema_page();

        // Create a row with columns in random order
        let unordered_row = vec![
            ("description".to_string(), "great for snacking".to_string()),
            ("name".to_string(), "Mandarin".to_string()),
            ("id".to_string(), "1".to_string()),
        ];

        let unordered_row = unordered_row.iter().map(|cl| cl).collect::<Vec<_>>();

        // Define desired column order
        let column_order = &["name", "description", "id"];

        // Expected ordered result
        let expected_ordered = vec![
            ("name".to_string(), "Mandarin".to_string()),
            ("description".to_string(), "great for snacking".to_string()),
            ("id".to_string(), "1".to_string()),
        ];

        // Get actual ordered result
        let actual_ordered = schema_page
            .borrow()
            .order_row_columns(column_order, unordered_row);

        assert_eq!(
            actual_ordered, expected_ordered,
            "Columns were not ordered correctly according to specified order"
        );
    }
    #[test]
    fn test_get_varints_byte_array() {
        let db = get_db_instance("superheroes".into());
        let file = &mut db.get_file();
        let schema_page = db.get_schema_page();
        let page = schema_page.borrow();
        let row_offset = 3728;
        let byte_array_header_size = 7usize;
        let actual_res: Vec<usize> =
            page.get_varints_byte_array(file, byte_array_header_size, row_offset);
        let expected = vec![7, 17, 23, 23, 1, 193];
        assert!(actual_res.iter().eq(expected.iter()))
    }
}
