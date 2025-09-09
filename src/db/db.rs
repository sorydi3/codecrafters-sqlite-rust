use crate::db::header::DatabaseHeader;
use crate::db::page::Page;
use std::cell::{RefCell, RefMut};
use std::fs::File;
use std::sync::Arc;

#[allow(dead_code)]
pub struct Db {
    file: Arc<File>,
    header: Arc<DatabaseHeader>,
    schema_page: Arc<RefCell<Page>>,
    pages: Arc<Vec<Box<Page>>>, // all other pages
}

impl Db {
    pub fn new(db_path: String) -> Self {
        let mut file = Arc::new(File::open(db_path).expect("Unable to open file"));
        let page_number = 1usize;
        let header = Arc::new(DatabaseHeader::new_(&mut file));
        let page_size = header.page_info().0;

        // read head
        let schema_page = Arc::new(RefCell::new(Page::new__(
            &mut file.clone(),
            page_number,
            page_size as usize,
        )));
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
    #[allow(dead_code)]
    pub fn get_file(&self) -> Arc<File> {
        self.file.clone()
    }

    pub fn get_table_count_schema_page(&self) -> usize {
        self.schema_page.borrow().get_table_count() as usize
    }

    pub fn get_schema_page(&self) -> Arc<RefCell<Page>> {
        self.schema_page.clone()
    }

    pub fn display_columns(&mut self, columns: &[&str], table_name: String) {
        self.schema_page
            .borrow_mut()
            .display_table_colums(&mut self.file, columns, table_name);
    }

    pub fn fill_tables_rows(&mut self) -> std::sync::Arc<RefMut<'_, Page>> {
        Arc::new(self.schema_page.borrow_mut())
    }
}
