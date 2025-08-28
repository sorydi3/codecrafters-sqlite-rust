use std::io::*;
use std::str;

use std::fs::File;
use std::sync::Arc;

pub struct DatabaseHeader {
    header_str: String, // 16 bytes
    page_size: u16,     // 2 bytes offset 16
}

pub const HEADER_BYTES_SIZE: u8 = 100;

impl DatabaseHeader {
    pub const SIZE_HEAD_STR: u8 = 15; //Bytes
    pub const SIZE_PAGE_SIZE: u8 = 2; //Bytes
    pub const OFFSET_PAGE_SIZE: u8 = 16;

    #[allow(dead_code)]
    pub fn new(database_header: &[u8]) -> Self {
        let header_str = str::from_utf8(&database_header[0..15])
            .expect("AN ISSUE HAPPENED")
            .to_string();

        let page_size: u16 = u16::from_be_bytes([database_header[16], database_header[17]]);
        Self {
            header_str,
            page_size,
        }
    }

    pub fn new_(file: &mut Arc<File>) -> Self {
        let header_str = str::from_utf8(&DatabaseHeader::buffer_read(
            DatabaseHeader::SIZE_HEAD_STR as usize,
            0,
            file,
        ))
        .expect("AN ISSUE HAPPENED")
        .to_string();

        let buff_page_size: Vec<u8> = DatabaseHeader::buffer_read(
            DatabaseHeader::SIZE_PAGE_SIZE as usize,
            DatabaseHeader::OFFSET_PAGE_SIZE as usize,
            file,
        );
        let page_size: u16 = u16::from_be_bytes([buff_page_size[0], buff_page_size[1]]);

        Self {
            header_str,
            page_size,
        }
    }

    fn buffer_read(buff_size: usize, offset: usize, file: &mut Arc<File>) -> Vec<u8> {
        let mut buff = vec![0; buff_size];

        file.seek(SeekFrom::Start(offset as u64))
            .expect("seek failed from buffer_read() ");

        file.read_exact(&mut buff)
            .expect("read_exact failed from buffer_read() ");

        buff
    }

    pub fn page_info(&self) -> (u16, String) {
        (self.page_size, self.header_str.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_header_info() {
        let file = Arc::new(
            File::open("/Users/ollaid/codecrafters/codecrafters-sqlite-rust/sample.db")
                .expect("FAILED TO OPEN"),
        );
        let dbheader: DatabaseHeader = DatabaseHeader::new_(&mut file.clone());
        let (page_size, string_header) = dbheader.page_info();
        assert_eq!(string_header, "SQLite format 3");
        assert_eq!(page_size, 4096);
    }
}
