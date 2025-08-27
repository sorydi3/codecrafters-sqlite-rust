use anyhow::{bail, Result};
use core::str;
use std::fs::File;
use std::io::prelude::*;
use std::usize;

#[derive(Debug, Clone)]
enum PageType {
    LEAFINDEX,
    LEAFTABLE,
    INTERIORINDEX,
    INTERIORTABLE,
    UNKNOWNTYPE,
}

#[derive(Debug, Copy, Clone, Default)]
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

struct Table {
    rows: Vec<Box<Row>>,
}

type CellField = (usize, Box<CellFieldType>, Vec<u8>, usize, usize, String); //(offset,prevtype,cur_bytes,type,size)

#[derive(Debug, Clone)]

enum CellFieldType {
    Na,
    RecordSize(Option<CellField>),
    RowId(Option<usize>),
    SizeCellHeader(Option<CellField>),
    SchemaType(Option<CellField>),
    SchemaName(Option<CellField>),
    SchemaTableName(Option<CellField>),
    SchemaRootpage(Option<CellField>),
    SchemaSQL(Option<CellField>),
}

impl Default for CellFieldType {
    fn default() -> Self {
        Self::default()
    }
}

impl CellFieldType {
    fn get_value(&self) -> String {
        match &self {
            CellFieldType::RecordSize(cellfield) => "".into(),
            _ => "".into(),
        }
    }

    fn get_size(&self) -> usize {
        match &self {
            CellFieldType::RowId(_) => {
                1 // THE SIZE ALWAYS IS ONE , DOES NOT CHANGE
            }
            CellFieldType::RecordSize(cell) => {
                let value = cell.as_ref().unwrap();
                value.0 // return the offeset
            }

            // TODO
            _ => 0,
        }
    }
}

#[derive(Debug, Clone, Default)]
struct RecordHeader {
    payload: Vec<u8>,
    record_size_value: CellFieldType, //(bytes,value,size)
    record_type_value: CellFieldType,
    record_name_value: CellFieldType,
    record_table_name_value: CellFieldType,
}

impl RecordHeader {
    const OFFESET_VALUE: u8 = 7;
    fn new(payload: &[u8]) -> Self {
        Self {
            payload: payload.to_vec(),
            ..Default::default()
        }
    }

    fn decode_var_int(&self, offset: usize) -> Option<(Vec<u8>, usize)> {
        let mut res = vec![];
        let mut it = offset;
        let mut value: u64 = 0;
        let mut found = false;
        while !found && it < 9 {
            res.push(self.payload[it]);
            // construct the final value by joining bits
            let data_bits = dbg!(self.payload[it] & 0b0111_1111) as u64;
            value = dbg!((value << 7) | data_bits);
            // test if last bit is set
            if !(self.payload[it] & 0b1000_0000 == 0b1000_0000) {
                // test if last bit is set
                found = true
            }
            it += 1;
        }
        Some((res, value as usize))
    }

    fn set_record_field(
        &self,
        offset: usize,
        prevType: CellFieldType,
        curr_celltype: CellFieldType,
    ) -> CellFieldType {
        if let Some(decoded_var_int) = self.decode_var_int(offset) {
            let (recordtype, sizevalue) = self.parse_record_header(decoded_var_int.1 as u8);
            let value = self.read_bytes(sizevalue, offset);
            match curr_celltype {
                CellFieldType::RecordSize(None) => CellFieldType::RecordSize(Some((
                    offset,
                    Box::new(prevType),
                    decoded_var_int.0,
                    decoded_var_int.1,
                    sizevalue,
                    value,
                ))),
                CellFieldType::RowId(None) => CellFieldType::RowId(Some(offset)),
                CellFieldType::SchemaType(None) => CellFieldType::SchemaType(Some((
                    offset,
                    Box::new(prevType),
                    decoded_var_int.0,
                    decoded_var_int.1,
                    sizevalue,
                    value,
                ))),
                _ => CellFieldType::Na,
            }
        } else {
            CellFieldType::Na
        }
    }

    fn set_values(&mut self, _file: &mut File, _cell_offset: usize) -> &mut Self {
        println!("SETTING CELLS VALUES");
        // set the record size offset equal zero

        self.record_size_value =
            self.set_record_field(0, CellFieldType::Na, CellFieldType::RecordSize(None));
        let record_size = self.set_record_field(
            self.record_size_value.get_size(),
            CellFieldType::Na,
            CellFieldType::RecordSize(None),
        );
        let record_schema_type = self.set_record_field(
            record_size.get_size() + record_size.get_size(),
            record_size,
            CellFieldType::SchemaType(None),
        );

        let mut offset = 0;
        (1..6)
            .map(|i| {
                let res = self.decode_var_int(offset);
                offset = offset + res.unwrap().0.len();

                if let Some((bytes, value)) = self.decode_var_int(offset) {
                    (i as usize, self.parse_record_header(value as u8).1)
                } else {
                    (0 as usize, 0 as usize)
                }
            })
            .collect::<Vec<(usize, usize)>>();

        //self.parse_record_header(self.payload[4]).1
        // set the size of each column
        // set the value of each column
        /*
        self.record_type_value.0 = self.parse_record_header(self.payload[1]);
        self.record_name_value.0 = self.parse_record_header(self.payload[2]);
        self.record_table_name_value.0 = self.parse_record_header(self.payload[3]);
        self.record_type_value.1 = self.read_bytes(
            &mut vec![0; self.record_type_value.0.1],
            cell_offset + RecordHeader::OFFESET_VALUE as usize
        );
        let mut new_offset: usize = cell_offset as usize
            + RecordHeader::OFFESET_VALUE as usize
            + self.record_type_value.0.1;
        self.record_name_value.1 = self.read_bytes(
            &mut vec![0; self.record_name_value.0.1],
            new_offset
        );
         */

        /*
        let new_offset = RecordHeader::OFFESET_VALUE as usize
            + self.record_type_value.0 .1
            + self.record_name_value.0 .1;
        self.record_table_name_value.1 =
            self.read_bytes(self.record_table_name_value.0 .1, new_offset);
         */

        self
    }

    fn read_bytes(&self, size: usize, offset: usize) -> String {
        //println!("{:x?}",&self.payload[offset..offset+size]);
        String::from_utf8_lossy(&self.payload[offset..offset + size]).to_string()
    }

    fn parse_record_header(&self, serialType: u8) -> (RecordFieldType, usize) {
        let (field_type, field_size) = match serialType {
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
}

#[derive(Debug, Clone)]
struct Row {
    offset: u16,
    payload_size: u8,
    row_id: u16,
    payload: String,
}

#[derive(Debug, Clone)]
struct Page {
    type_page: PageType,
    table_count: u16,       // two bytes
    cell_content_area: u16, // two bytes
    cells: Vec<String>,
}

impl Page {
    fn new(database_page: &[u8]) -> Self {
        Self {
            type_page: Page::get_page_type(database_page[0]),
            table_count: u16::from_be_bytes([database_page[3], database_page[4]]),
            cell_content_area: u16::from_be_bytes([database_page[5], database_page[6]]),
            cells: vec![],
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
    fn fill_cell_vec(&mut self, file: &mut File) -> () {
        let schema_header_offeset = 8;
        let mut init_offset = OFFSET + schema_header_offeset;
        let res = file.seek(std::io::SeekFrom::Start(init_offset as u64));
        match res {
            Ok(res) => {
                let mut i = 1;
                let step = 2;

                while i <= self.table_count {
                    // iterate throught the cells of the schema table
                    let mut buffer: [u8; 2] = [0; 2];
                    let offset_value = file.read_exact(&mut buffer);

                    let row_offset = u16::from_be_bytes([buffer[0], buffer[1]]);

                    let res = self
                        .get_cell_value(file, row_offset)
                        .expect("FAILED TO READ CELL VALUE");
                    self.cells.push(res); // add the cells vector
                    //seek to the row data using the the current
                    //let offse_row_data = file.seek_relative(offset)
                    init_offset = init_offset + step;

                    if let Ok(new_pos_cursor) =
                        file.seek(std::io::SeekFrom::Start(init_offset as u64))
                    {
                        ();
                    }
                    i += 1;
                }
            }
            Err(x) => {
                println!("SOMTHING WENT WRONG!! {}", x)
            }
        }
    }
    
       fn display_cells(&self) {
           let res = self
               .cells
               .iter()
               .filter_map(|c| {
                  match **c != "sqlite_sequence".to_string() {
                    true =>  Some(c.clone()),
                    _ => None
                  }
               }).collect::<Vec<String>>();
               
               println!("{:?}",res.join(" "));


       }

    fn get_table_count(&self) -> u16 {
        self.table_count
    }

    fn get_varint_buffer(file: &mut File, offset: usize, size: usize) -> Vec<u8> {
        let mut buffer = vec![0; size];
        let _ = file.seek(std::io::SeekFrom::Start(offset as u64));
        file.read_exact(&mut buffer);
        buffer
    }

    fn decode_var_int(&self, offset: usize, file: &mut File) -> Option<(Vec<u8>, usize)> {
        file.seek(std::io::SeekFrom::Start(offset as u64));
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

    fn parse_record_header(&self, serialType: u8) -> (RecordFieldType, usize) {
        let (field_type, field_size) = match serialType {
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

    fn read_bytes(&self, file: &mut File, offset: usize, size: usize) -> String {
        //println!("{:x?}",&self.payload[offset..offset+size]);
        let mut buff = vec![0; size];
        file.seek(std::io::SeekFrom::Start(offset as u64)).expect("SEEK read_bytes() failed");

        file.read_exact(&mut buff).expect("read_exact() from read_bytes() failed ");

        let res = String::from_utf8_lossy(&buff).to_string();
        res
    }

    fn get_cell_value(
        // get the table_name_schema
        &mut self,
        file: &mut File,
        cell_offset: u16,
    ) -> Result<String, anyhow::Error> {
        let mut offset: usize = cell_offset as usize;
        //println!("PRINTING CELLS VALUES ");

        let mut payload_size_value = 0;
        let mut display_values = || {
            if let Some((bytes, value)) = self.decode_var_int(offset as usize, file) {
                payload_size_value = value;
                offset += bytes.len();
                //println!("PAYLOAD SIZE IS: {value:?}  OFFSET={offset:?}");
                (payload_size_value, offset)
            } else {
                (0, 0)
            }
        };

        display_values(); // Size of the record (varint)
        display_values(); //The rowid (safe to ignore)
        display_values(); // Size of record header (varint

        //let _ = self.parse_record_header(display_values().0 as u8).1;
        let mut size = self.parse_record_header(display_values().0 as u8).1;
        size += self.parse_record_header(display_values().0 as u8).1;

        let (_, size_schema_table_name) = self.parse_record_header(display_values().0 as u8);

        let _ = self.parse_record_header(display_values().0 as u8);

        let (_, schema_sql_offset) = display_values();
        let tbl_name_offset = schema_sql_offset + size;

        let res = self.read_bytes(file, tbl_name_offset, size_schema_table_name);
        Ok(res)
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
        ".tables" => {
            let mut file = File::open(&args[1])?;

            let mut buffer = [0; 8];

            file.seek(std::io::SeekFrom::Start(OFFSET as u64))
                .expect("SOMTHING WENT WRONG W8HILE SEEKING");

            let _ = file
                .read_exact(&mut buffer)
                .expect("SOMTHING WENT WRONG WHILE FILLING THE BUFFER");
            let mut schema_page = Page::new(&buffer);

            schema_page.fill_cell_vec(&mut file);

            schema_page.display_cells();
        }
        _ => bail!("Missing or invalid command passed: {}", command),
    }
    Ok(())
}

// https://blog.sylver.dev/build-your-own-sqlite-part-1-listing-tables?source=more_series_bottom_blogs

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_bytes_varint_2() {
        let response_bytes: &[u8; 2] = &[0x81, 0x47];
        let response_value = 199usize;
        let rec_header = RecordHeader::new(&[0x81, 0x47, 0x74, 0x61, 0x62]);
        let (bytes, value) = rec_header.decode_var_int(0).expect("DECODE VARINT FAILED");
        assert_eq!(bytes, response_bytes);
        assert_eq!(value, response_value);
    }

    #[test]
    fn test_set_values() {}

    #[test]
    fn test_get_bytes_varint_1() {
        let response_bytes = &[0x47];
        let response_value = 71usize;
        let rec_header = RecordHeader::new(&[0x47, 0x74, 0x61, 0x62]);
        let (bytes, value) = rec_header.decode_var_int(0).expect("DECODE VARINT FAILED");
        assert_eq!(bytes, response_bytes);
        assert_eq!(value, response_value);
    }
}
