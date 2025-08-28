use crate::db::header::DatabaseHeader;
use crate::db::page::Page;
use std::fs::File;
use std::sync::Arc;

#[allow(dead_code)]
pub struct Db {
    file: Arc<File>,
    header: Arc<DatabaseHeader>,
    schema_page: Arc<Page>,
    pages: Arc<Vec<Box<Page>>>, // all other pages
}

impl Db {
    pub fn new(db_path: String) -> Self {
        let mut file = Arc::new(File::open(db_path).expect("Unable to open file"));
        let page_number = 1usize;
        let header = Arc::new(DatabaseHeader::new_(&mut file));
        let page_size = header.page_info().0;

        // read head
        let schema_page = Arc::new(Page::new__(
            &mut file.clone(),
            page_number,
            page_size as usize,
        ));
        let pages = Arc::new(vec![]);

        Self {
            file,
            header,
            schema_page,
            pages,
        }
    }

    pub fn get_page_size(&self) -> usize {
        self.header.page_info().0 as usize
    }

    pub fn get_table_count_schema_page(&self) -> usize {
        self.schema_page.get_table_count() as usize
    }

    pub fn get_schema_page(&self) -> Arc<Page> {
        self.schema_page.clone()
    }
}
