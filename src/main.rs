use anyhow::{bail, Result};
use core::str;
use std::error::Error;
use std::fs::File;
use std::io::prelude::*;

use std::convert::TryInto; // for converting from slice to an array

#[derive(Debug, Clone)]
enum PageType {
    LEAFINDEX,
    LEAFTABLE,
    INTERIORINDEX,
    INTERIORTABLE,
    UNKNOWNTYPE,
}

#[derive(Debug, Copy, Clone,Default)]
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

#[derive(Debug, Clone, Default)]
struct RecordHeader {
    payload: Vec<u8>,
    record_size_value: (u8, u8), //(size,value)
    record_type_value: ((RecordFieldType, usize), String),
    record_name_value: ((RecordFieldType, usize), String),
    record_table_name_value: ((RecordFieldType, usize), String),
}

impl RecordHeader {
    const OFFESET_VALUE: u8 = 7;
    fn new(payload: &[u8]) -> Self {
        Self {
            payload: payload.to_vec(),
            ..Default::default()
        }
    }

    fn set_values(&mut self, file: &mut File, cell_offset: usize) -> &mut Self {
        //self.parse_record_header(self.payload[4]).1
        //println!("PAYLOAD: {:x?}",&self.payload);
        //println!("PAYLOAD: {:?}",String::from_utf8_lossy(&self.payload).to_string());

        println!("SIZE RECORD HEADER size:: {:?}",self.payload[0]);
        println!("SIZE RECORD name:: {:?} tbl: {:?}",self.payload[2],self.payload[3]);
        println!("SIZE RECORD type:: {:?}",self.payload[4]);
        println!("SIZE RECORD sql:: {:?}",u16::from_be_bytes([self.payload[5],self.payload[6]]));
        // set the size of each column
        self.record_type_value.0 = self.parse_record_header(self.payload[1]);
        self.record_name_value.0 = self.parse_record_header(self.payload[2]);
        self.record_table_name_value.0 = self.parse_record_header(self.payload[3]);
        // set the value of each column
        /*
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
        let new_offset = RecordHeader::OFFESET_VALUE as usize + self.record_type_value.0.1+self.record_name_value.0.1;
        self.record_table_name_value.1 = self.read_bytes(
            self.record_table_name_value.0.1,
            new_offset,
        );

        self
    }

    fn read_bytes(&self, size: usize, offset: usize) -> String {
        //println!("{:x?}",&self.payload[offset..offset+size]);
        String::from_utf8_lossy(&self.payload[offset..offset+size]).to_string()
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
    cells: Vec<Box<Option<RecordHeader>>>,
}

impl Page {
    fn new(database_page: &[u8]) -> Self {
        Self {
            type_page: Page::get_page_type(database_page[0]),
            table_count: u16::from_be_bytes([database_page[3], database_page[4]]),
            cell_content_area: u16::from_be_bytes([database_page[5], database_page[6]]),
            cells: vec![Box::new(None)],
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

                    self.print_cell_values(file, row_offset, init_offset as u16);
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
        let _ = self
            .cells
            .iter()
            .filter(|c| {
                let value = (***c).clone();

                match value {
                    Some(res) => res.record_table_name_value.1 != "qlite_sequences".to_string(),
                    _ => false,
                }
            }).map(|v| {
                let value = (**v).clone();
                match value {
                    Some(res) => {
                        let val = &res.record_table_name_value.1;
                        println!("{:?}",val);
                        val.clone()
                    },
                    _ => "".to_string(),
                }
            }).collect::<Vec<_>>();
    }

    fn get_table_count(&self) -> u16 {
        self.table_count
    }

    fn print_cell_values(&mut self, file: &mut File, cell_offset: u16, prev_offset: u16) -> Result<(),anyhow::Error> {
        let _ = file.seek(std::io::SeekFrom::Start(cell_offset as u64));

        let mut payload_size_buff = [0; 1];
        let _ = file.read_exact(&mut payload_size_buff);
        let payload_size_value = u8::from_be_bytes(payload_size_buff);

        let mut rowid_buff = [0; 1];
        let _ = file.seek(std::io::SeekFrom::Start((cell_offset as u64) + 1))?;
        let _ = file.read_exact(&mut rowid_buff);
        let row_id_value = u8::from_be_bytes(rowid_buff);

        let mut payload_buff = vec![0; payload_size_value as usize];
        let new_pos = file
            .seek(std::io::SeekFrom::Start((cell_offset as u64) + 2))
            .expect("SEEK FAILED");
        let _ = file.read_exact(&mut payload_buff);
        let payload = String::from_utf8_lossy(&payload_buff[..]);
        file.seek(std::io::SeekFrom::Start(prev_offset as u64))?;

        let mut record = RecordHeader::new(&payload_buff[..]);
        record.set_values(file, cell_offset as usize);
        //println!("{:?}", record.record_name_value.1);
        self.cells.push(Box::new(Some(record)));
        Ok(())
        
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
