use crate::flat::{Immediate, Row, TableSchema, FieldDescription};
use crate::store::{Table, TableSchemaEx};
use crate::utils::create_memory_mapping;
use std::fmt::{Display, Formatter};
use std::io::{Error, Read};
use std::sync::atomic::{AtomicUsize, Ordering, AtomicIsize, AtomicU32, AtomicBool};
use std::cell::{UnsafeCell, Cell};
use easybench::{bench, bench_env};
use byteorder::{LittleEndian, ByteOrder};
use std::borrow::{BorrowMut, Borrow};
use std::ptr::write_bytes;

///
/// data is an infinite array representing data, at the beginning of each
/// there is a u16 length and the content which must be < 64kb.
///
/// index is split in 64 bytes memory blocks (size of the cache line)
///   - 64 bit representing the absolute memory address
///   - 24 bit representing the relative memory address (64-8)/3 = 18 entries 2 wasted bytes
/// OR- 16 bit representing the size of entry (64-8)/2 = 28 entries 0 wasted bytes
///     but you must sum up to 28 integers to retrieve the pointer
/// having ~ 3.5 byte overhead per entry
///
/// for this reason it is crucial for the index to aligned to memory page 4096
/// each entry access should only have 1 cache line access regardless you are looking for
/// the first entry or the last entry of a block.
///
/// if a relative index have two special values:
///     0xFFFF it means it has been deleted
///     0xFFFE it means it's zero length
///
/// deleted entries memory won't be reclaimed, reusing memory would break
/// memory locality when iterating the index at that point the only real
/// solution to reclaim that memory is a controlled reboot of the instance
///
struct ColumnDataBlob {
  data: Box<&'static mut [u8]>,
  index: Box<&'static mut [u8]>,
  next_data_position: AtomicUsize,
  next_index_to_initialize: AtomicU32,
  deleted_memory: u64,
  is_initializing_data_block: AtomicBool
}

const ENTRY_PER_BLOB_BLOCK: u32 = 18;

impl ColumnDataBlob {
  fn create() -> Self {
    ColumnDataBlob {
      data: Box::new(create_memory_mapping()),
      index: Box::new(create_memory_mapping()),
      next_data_position: AtomicUsize::new(0),
      next_index_to_initialize: AtomicU32::new(0),
      is_initializing_data_block: AtomicBool::new(false),
      deleted_memory: 0,
    }
  }

  fn get(&self, index: u32) -> Option<&[u8]> {
    if index >= self.next_index_to_initialize.load(Ordering::Relaxed) {
      return None
    }

    let (relative_offset, data_offset) = self.get_data_position(index);

    if relative_offset == 0x00FFFFFF /* 0x00FFFFFF=DELETED */ {
      None
    } else {
      let length = LittleEndian::read_u16(
        &self.data[data_offset..data_offset + 2]
      ) as usize;

      Some(&self.data[data_offset + 2..data_offset + length + 2])
    }
  }

  #[inline]
  fn get_data_position(&self, index: u32) -> (u32,usize) {
    let index_block_offset = ColumnDataBlob::calc_index_block_begin_offset(index);
    let data_block_begin = LittleEndian::read_u64(
      &self.index[index_block_offset..index_block_offset + 8]
    ) as usize;

    let index_offset = ColumnDataBlob::calc_index_offset(index);
    let data_block_offset = LittleEndian::read_u24(
      &self.index[index_offset..index_offset + 3]
    );

    (data_block_offset as u32, data_block_begin + data_block_offset as usize)
  }

  #[inline]
  fn allocate_data(&mut self, size: usize) -> usize {
    loop {
      let current = self.next_data_position.load(Ordering::SeqCst);

      let next_data_index = self.next_data_position.compare_and_swap(
        current,
        current + size,
        Ordering::Relaxed,
      );

      if current == next_data_index {
        return current;
      }
    }
  }

  ///
  /// return the offset to the position
  /// which contain the data address
  ///
  #[inline]
  fn calc_index_offset(index: u32) -> usize {
    (index as usize / 18) * 64 + //numbers of blocks
      3 * (index % 18) as usize + 8 //relative to the single block
  }

  #[inline]
  fn calc_index_block_begin_offset(index: u32) -> usize {
    (index as usize / 18) * 64 as usize
  }

  #[inline]
  fn assert_block(&mut self,index: u32) {
    let next_index_to_initialize = self.next_index_to_initialize.load(
      Ordering::Relaxed
    );

    if next_index_to_initialize > index {
      return;
    }

    loop {
      let result = self.is_initializing_data_block.compare_and_swap(
        false, true,
        Ordering::SeqCst
      );

      let next_index_to_initialize = self.next_index_to_initialize.load(
        Ordering::SeqCst
      );

      if next_index_to_initialize > index {
        if result == false {
          self.is_initializing_data_block.store(false, Ordering::Relaxed)
        }
        return;
      }

      if result == false {
        let abs_data_offset = self.next_data_position.load(Ordering::SeqCst);
        let block_begin_offset = ColumnDataBlob::calc_index_block_begin_offset(index);
        unsafe { write_bytes(&mut self.index[block_begin_offset], 0xFF, 64); }
        LittleEndian::write_u64(
          &mut self.index[block_begin_offset..block_begin_offset + 8],
          abs_data_offset as u64,
        );
        self.next_index_to_initialize.store(next_index_to_initialize+18, Ordering::Relaxed);
        self.is_initializing_data_block.store(false, Ordering::Relaxed);
        return;
      }
    }
  }

  fn insert(&mut self, index: u32, immediate: &Immediate) -> u32 {
    assert!(immediate.blob().unwrap().len() < 31 * 1024);

    self.assert_block(index);
    let blob = immediate.blob().unwrap();
    let blob_len = blob.len();
    let abs_data_offset = self.allocate_data(
      blob_len + 2
    );

    let abs_data_block_begin: usize;
    let abs_index_offset = ColumnDataBlob::calc_index_offset(index);

    {
      let block_begin_offset = ColumnDataBlob::calc_index_block_begin_offset(index);
      abs_data_block_begin = LittleEndian::read_u64(
        &mut self.index[block_begin_offset..block_begin_offset + 8],
      ) as usize;
    }

    //write the length of blob in data
    LittleEndian::write_u16(
      &mut self.data[abs_data_offset..abs_data_offset + 2],
      blob_len as u16,
    );

    //write the blob itself in data
    unsafe {
      libc::memcpy(
        self.data.as_mut_ptr().offset(
          (abs_data_offset + 2) as isize
        ) as *mut libc::c_void,
        blob.as_ptr() as *const libc::c_void,
        blob_len,
      );
    }

    //TODO: insert memory barrier here
    //to avoid index pointing toward an non-initialed space

    assert!(abs_data_offset >= abs_data_block_begin);
    assert!(abs_data_offset - abs_data_block_begin < 0x1000000);

    LittleEndian::write_u24(
      &mut self.index[abs_index_offset as usize..abs_index_offset + 3],
      (abs_data_offset - abs_data_block_begin) as u32,
    );

    index
  }
}

struct RamColumn {
  id: u8,
  type_: crate::flat::FieldType,
  size: u16,
  blob_data: Option<Cell<ColumnDataBlob>>
}

impl RamColumn {
  fn insert(&self, index: u32, immediate: &Immediate) -> u32 {
    self.blob_data().insert(index, immediate)
  }

  fn blob_data(&self) -> &mut ColumnDataBlob {
    unsafe { &mut *self.blob_data.as_ref().unwrap().as_ptr() }
  }

  
  fn get_blob(&self, index: u32) -> Option<&[u8]> {
    self.blob_data().get(index)
  }

  fn create(field: &FieldDescription) -> Self {
    RamColumn {
      id: field.id(),
      type_: field.type_(),
      size: field.size_(),
      blob_data: Some(Cell::new(ColumnDataBlob::create()))
    }
  }
}

pub struct RamTable<'ram> {
  schema: TableSchemaEx<'ram>,
  columns: UnsafeCell<Vec<RamColumn>>,
}

impl RamTable<'_> {
  pub fn create(schema: TableSchema) -> Self {
    let mut table = RamTable {
      schema: TableSchemaEx::create(schema),
      columns: UnsafeCell::new(Vec::new()),
    };
    for field in schema.fields() {
      table.columns().push(RamColumn::create(&field));
    }
    table
  }

  fn columns(&self) -> &mut Vec<RamColumn> {
    unsafe {
      &mut *self.columns.get()
    }
  }
}

unsafe impl Sync for RamTable<'_> {}

impl<'ram_table> Table<'ram_table> for RamTable<'ram_table> {
  fn insert(&self, row: &Row<'_>) {
    let mut column_index = 0;
    let row_index = 0;
    for value in row.data() {
      if let Some(mut column) = self.columns().get_mut(column_index) {
        column.insert(row_index, &value);
      }
      column_index += 1;
    }
  }
}

impl<'a> Display for RamTable<'a> {
  fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
    f.write_str(self.schema.schema().name())
  }
}

#[cfg(test)]
mod tests {
  use crate::ram::RamColumn;
  use crate::flat::{FieldDescription, FieldDescriptionArgs, TableSchemaArgs, CreateCommandArgs, PacketArgs, Command, get_root_as_packet, TableSchema, ImmediateArgs, ImmediateBuilder, Immediate, FieldType};
  use crate::flat;
  use crate::flat::FieldType::Blob;
  use flatbuffers::FlatBufferBuilder;
  use easybench::{bench_env, bench, bench_gen_env};
  use std::sync::atomic::{AtomicU32, Ordering};
  use std::thread;
  use std::sync::Arc;

  fn build_immediate_string<'a>(builder: &'a mut FlatBufferBuilder, string: &str) -> Immediate<'a> {
    let string_offset = builder.create_vector(
      string.as_bytes()
    );

    let root = Immediate::create(
      builder,
      &ImmediateArgs {
        type_: FieldType::Blob,
        blob: Some(string_offset),
        num: 0,
      },
    );

    builder.finish_minimal(root);
    flatbuffers::get_root::<Immediate>(&*builder.finished_data())
  }

  fn build_schema<'a>(builder: &'a mut FlatBufferBuilder) -> TableSchema<'a> {
    build_create_table(builder);
    let data = builder.finished_data();
    let packet = get_root_as_packet(data);

    packet.command_as_create().unwrap().schema()
  }

  fn build_create_table(builder: &mut flatbuffers::FlatBufferBuilder) {
    let from = Some(builder.create_string("from"));
    let to = Some(builder.create_string("to"));
    let message = Some(builder.create_string("message"));

    let fields: [flatbuffers::WIPOffset<FieldDescription>; 3] = [
      flat::FieldDescription::create(
        builder,
        &FieldDescriptionArgs {
          id: 0x01,
          name: from,
          type_: Blob,
          size_: 32,
        },
      ),
      flat::FieldDescription::create(
        builder,
        &FieldDescriptionArgs {
          id: 0x02,
          name: to,
          type_: Blob,
          size_: 32,
        },
      ),
      flat::FieldDescription::create(
        builder,
        &FieldDescriptionArgs {
          id: 0x03,
          name: message,
          type_: Blob,
          size_: 4096,
        },
      )
    ];

    let fields_vector = Option::Some(
      builder.create_vector(&fields)
    );
    let messages = Some(builder.create_string("messages"));
    let schema = flat::TableSchema::create(
      builder,
      &TableSchemaArgs {
        id: 0x01,
        name: messages,
        fields: fields_vector,
      },
    );

    let engine = Some(builder.create_string("ram"));
    let create_command = flat::CreateCommand::create(
      builder,
      &CreateCommandArgs {
        schema: Some(schema),
        engine: engine,
      },
    );

    let packet = flat::Packet::create(
      builder,
      &PacketArgs {
        crc: 0,
        version: 1,
        timeout: 1000,
        command_type: Command::Create,
        command: Some(create_command.as_union_value()),
      },
    );

    builder.finish(packet, None);
  }

  //TESTS

  #[test]
  fn test_simple_insert() {
    let mut builder_immediate = flatbuffers::FlatBufferBuilder::new_with_capacity(
      2048
    );
    let immediate = build_immediate_string(
      &mut builder_immediate,
      "hello",
    );

    let column = create_column();

    column.insert(0, &immediate);

    assert_eq!(column.get_blob(0), Some("hello".as_bytes()));
    assert_eq!(column.get_blob(1), None);
  }

  #[test]
  fn bench_insert_and_get(){
    let mut builder_immediate = flatbuffers::FlatBufferBuilder::new_with_capacity(
      2048
    );
    let immediate = build_immediate_string(
      &mut builder_immediate,
      "hello",
    );

    let column = create_column();

    let write_index = AtomicU32::new(0);
    {
      let result = bench(|| {
        column.insert(write_index.fetch_add(1, Ordering::Relaxed), &immediate);
      });
      print!("\nWrite sequential: {}\n", result);
    }

    {
      let loaded_write_index = write_index.load(Ordering::Relaxed);
      let read_index = AtomicU32::new(0);
      let result = bench(|| {
        column.get_blob(read_index.fetch_add(1, Ordering::Relaxed) % loaded_write_index );
      });
      print!("\nRead sequential: {}\n", result);
    }

    {
      let loaded_write_index = write_index.load(Ordering::Relaxed);
      let result = bench(|| {
        let index = rand::random::<u32>() % loaded_write_index;
        let blob = column.get_blob(index);
        assert_eq!(blob, Some("hello".as_bytes()));
      });
      print!("\nRead random: {}\n", result);
    }
  }

  #[test]
  fn multi_thread_insert_and_get() {
    let mut builder_immediate = flatbuffers::FlatBufferBuilder::new_with_capacity(
      2048
    );
    let immediate = build_immediate_string(
      &mut builder_immediate,
      "hello",
    );

    let column = Arc::new(create_column());

    let write_index = AtomicU32::new(0);
    for _ in 0..10 {
      let column = column.clone();
      let result = thread::spawn(move || {
        let index = write_index.fetch_add(1, Ordering::SeqCst);
        column.insert(index, &immediate);
        if index >= 4000 {
          return
        }
      });
    }

    let read_index = AtomicU32::new(4000);
    for _ in 0..10 {
      let result = thread::spawn(|| {
        let index = write_index.fetch_sub(1, Ordering::SeqCst);
        if index < 0 {
          return;
        }
        if let Some(x) = column.get_blob(index) {
          assert_eq!(x, "hello!".as_bytes())
        }
      });
    }
  }

  fn create_column() -> RamColumn {
    let mut builder = flatbuffers::FlatBufferBuilder::new_with_capacity(
      2048
    );
    let schema = build_schema(&mut builder);
    let mut column = RamColumn::create(
      &schema.fields().get(0)
    );
    column
  }
}