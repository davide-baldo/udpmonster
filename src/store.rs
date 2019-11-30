use crate::flat;
use std::collections::HashMap;
use std::sync::Arc;
use crate::flat::{TableSchema, Row};
use crate::flat::FieldType::Blob;
use std::fmt::{Display, Formatter, Error};
use std::marker::PhantomData;
use std::borrow::BorrowMut;
use std::ops::Deref;
use std::cell::UnsafeCell;

pub trait Table<'table>: Display + Sync {
  fn insert(&self,row: &crate::flat::Row );
}

struct TableSchemaEx<'ex> {
  data: Box<[u8]>,
  _phantom: PhantomData<&'ex ()>,
}

impl<'ex> TableSchemaEx<'ex> {
  fn schema(&self) -> TableSchema<'_> {
    flatbuffers::get_root::<TableSchema>(&*self.data)
  }
}

impl<'ex> TableSchemaEx<'ex> {
  fn create(schema: TableSchema) -> TableSchemaEx<'ex> {
    let mut builder = flatbuffers::FlatBufferBuilder::new_with_capacity(
      2048
    );
    let root = schema.write_to(&mut builder);
    builder.finish_minimal(root);

    TableSchemaEx {
      data: Vec::from(builder.finished_data()).into_boxed_slice(),
      _phantom: PhantomData
    }
  }
}

struct RamColumn {
  field_id: u16,
  field_type: crate::flat::FieldType,
  field_size: u16
}

impl RamColumn {
  fn insert() -> u32 {
    unimplemented!()
  }
}

struct RamTable<'ram> {
  schema: TableSchemaEx<'ram>,
  //columns: Vec<RamColumn>
}

unsafe impl Sync for RamTable<'_> {}

impl<'ram_table> Table<'ram_table> for RamTable<'ram_table> {
  fn insert(&self, _row: &Row<'_>) {
    unimplemented!()
  }
}

impl<'a> Display for RamTable<'a> {
  fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
    f.write_str(self.schema.schema().name())
  }
}

pub struct Store {
  table_map: UnsafeCell<HashMap<u16, Arc<RamTable<'static>>>>,
}

impl Store {
  pub fn create<'a>() -> Store {
    Store {
      table_map: UnsafeCell::new(HashMap::new()),
    }
  }

  pub fn create_table(&mut self, schema: TableSchema, engine_name: &str) -> Option<Arc<dyn Table<'static> + 'static>> {
    let table: Arc<RamTable<'static>> = match engine_name {
      "ram" => {
        Arc::new(RamTable {
          schema: TableSchemaEx::create(schema)
        })
      },
      _ => return None
    };
    self.map().insert(schema.id(), Arc::clone(&table));
    Some(table)
  }

  fn map(&self) -> &'static mut HashMap<u16, Arc<RamTable<'static>>> {
    unsafe{ &mut (*self.table_map.get()) }
  }

  pub fn drop_table(&self, table_id: u16) -> bool {
    match self.map().remove(&table_id) {
      Some(_table) => true,
      _ => false
    }
  }

  pub fn get_table(&self, table_id: u16) -> Option<Arc<dyn Table<'static> + 'static>>  {
    match self.map().get(&table_id) {
      Some(val) => Some(val.clone()),
      None => None
    }
  }
}
