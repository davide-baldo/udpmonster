use crate::flat::{Immediate, Row, TableSchema, FieldDescription, ImmediateArgs, FieldType, RowArgs};
use crate::store::{Table, TableSchemaEx};
use crate::utils::{create_memory_mapping, create_column};
use std::fmt::{Display, Formatter};
use std::sync::atomic::{AtomicUsize, Ordering, AtomicU32, AtomicBool, fence, compiler_fence};
use std::cell::{UnsafeCell, Cell};
use byteorder::{LittleEndian, ByteOrder};
use std::ptr::write_bytes;
use flatbuffers::{WIPOffset, FlatBufferBuilder, Vector};
use std::thread;

///
/// data is an infinite array representing data, at the beginning of each
/// there is a u16 length and the content which must be < 64kb.
///
/// index is split in 64 bytes memory blocks (size of the cache line)
///   - 64 bit representing the absolute memory address
///   - 24 bit representing the relative memory address (64-8)/3 = 18 entries 2 wasted bytes
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
  is_initializing_data_block: AtomicBool,
  block_split: u8,
  block_num: u8,
}

unsafe impl Sync for ColumnDataBlob { }

const ENTRY_PER_BLOB_BLOCK: u32 = 18;

impl ColumnDataBlob {
  fn create(block_split: u8, block_num: u8) -> Self {
    ColumnDataBlob {
      data: Box::new(create_memory_mapping()),
      index: Box::new(create_memory_mapping()),
      next_data_position: AtomicUsize::new(0),
      next_index_to_initialize: AtomicU32::new(0),
      is_initializing_data_block: AtomicBool::new(false),
      deleted_memory: 0,
      block_split: block_split,
      block_num: block_num,
    }
  }

  fn size(&self) -> usize {
    self.next_data_position.load(Ordering::Relaxed)
  }

  #[inline]
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

    let (_,index_offset) = ColumnDataBlob::calc_index_offset(index);
    let data_block_offset = LittleEndian::read_u24(
      &self.index[index_offset..index_offset + 3]
    );

    (data_block_offset as u32, data_block_begin + data_block_offset as usize)
  }

  #[inline]
  fn allocate_data(&mut self, size: usize) -> usize {
    let mut loop_count = 0;

    loop {
      let current = self.next_data_position.load(Ordering::Relaxed);

      let next_data_index = self.next_data_position.compare_and_swap(
        current,
        current + size,
        Ordering::Relaxed,
      );

      if current == next_data_index {
        return current;
      } else {
        loop_count += 1;
        if loop_count == 2 {
          thread::yield_now();
          loop_count = 0;
        }
      }
    }
  }

  ///
  /// return the offset to the position
  /// which contain the data address
  ///
  #[inline]
  fn calc_index_offset(index: u32) -> (usize,usize) {
    let block_begin_offset = (index as usize / 18) * 64 as usize; //numbers of blocks
    let relative_index =  3 * (index % 18) as usize + 8; //relative to the single block
    (block_begin_offset, block_begin_offset +relative_index)
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

    let mut loop_count = 0;
    loop {
      let result = self.is_initializing_data_block.compare_and_swap(
        false, true,
        Ordering::Relaxed
      );

      let next_index_to_initialize = self.next_index_to_initialize.load(
        Ordering::Relaxed
      );

      if next_index_to_initialize > index {
        if result == false {
          self.is_initializing_data_block.store(false, Ordering::Relaxed)
        }
        return;
      }

      if result == false {
        let block_begin_offset = ColumnDataBlob::calc_index_block_begin_offset(
          self.next_index_to_initialize.load(Ordering::Relaxed)
        );
        let abs_data_offset = self.next_data_position.load(Ordering::Relaxed);
        LittleEndian::write_u64(
          &mut self.index[block_begin_offset..block_begin_offset + 8],
          abs_data_offset as u64,
        );
        unsafe { write_bytes(&mut self.index[block_begin_offset+8], 0xFF, 64-8); }

        //make sure index is written before `next_index_to_initialize`
        fence(Ordering::Release);
        self.next_index_to_initialize.store(next_index_to_initialize+18, Ordering::Relaxed);
        self.is_initializing_data_block.store(false, Ordering::Relaxed);
      } else {
        loop_count += 1;
        if loop_count == 2 {
          thread::yield_now();
          loop_count = 0;
        }
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

    //make sure index is updated
    fence(Ordering::Acquire);

    let (block_begin_offset,abs_index_offset) = ColumnDataBlob::calc_index_offset(
      index
    );
    let abs_data_block_begin = LittleEndian::read_u64(
      &self.index[block_begin_offset..block_begin_offset + 8],
    ) as usize;

    if abs_data_offset < abs_data_block_begin {
      println!("{} {}", abs_data_offset, abs_data_block_begin)
    }

    assert!(abs_data_offset >= abs_data_block_begin);
    assert!(abs_data_offset - abs_data_block_begin < 16*1024*1024);

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

    //make sure that if the index is updated, also the data is updated
    fence(Ordering::Release);

    LittleEndian::write_u24(
      &mut self.index[abs_index_offset as usize..abs_index_offset + 3],
      (abs_data_offset - abs_data_block_begin) as u32,
    );

    index
  }
}

pub struct RamColumn {
  id: u8,
  type_: crate::flat::FieldType,
  size: u16,
  blob_data: Vec<Cell<ColumnDataBlob>>
}

unsafe impl Sync for RamColumn {}

///
/// This implementation split the column in 16 blocks to reduce concurrency
///
impl RamColumn {
  #[inline]
  pub fn insert(&self, index: u32, immediate: &Immediate) -> u32 {
    self.blob_data(index).insert(index >> 4, immediate)
  }

  pub fn size(&self, index: u32) -> usize {
    let mut size: usize = 0;
    for i in 0..16 {
      size += self.blob_data(i).size()
    }
    size
  }

  #[inline]
  pub fn blob_data(&self, index: u32) -> &mut ColumnDataBlob {
    unsafe { &mut *self.blob_data.get((index & 0xF) as usize).as_ref().unwrap().as_ptr() }
  }

  #[inline]
  pub fn get_blob(&self, index: u32) -> Option<&[u8]> {
    self.blob_data(index).get(index >> 4)
  }

  pub fn create(field: &FieldDescription) -> Self {
    let mut blob_vector = Vec::new();
    for i in 0..16 {
      blob_vector.push(Cell::new(ColumnDataBlob::create(
        16,
        i
      )))
    }
    
    RamColumn {
      id: field.id(),
      type_: field.type_(),
      size: field.size_(),
      blob_data: blob_vector
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

  #[inline]
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

  fn copy_row<'a>(
    &self, index: u32,
    builder: &mut FlatBufferBuilder<'a>,
    max_size: u16
  ) ->
    (bool, Option<flatbuffers::WIPOffset<Row<'a>>>) {

    let num_items = self.columns().len();
    let mut column_list: Vec<WIPOffset<Immediate>> = Vec::new();

    for i in (0..self.columns().len()).rev() {
      let column = self.columns().get(i).unwrap();
      let blob: Option<WIPOffset<Vector<u8>>>;

      if let Some(value) = column.get_blob(index) {
        if builder.used_space() + value.len() > max_size as usize {
          return (true, None);
        }
        blob = Some(builder.create_vector_direct(value));
      } else {
        blob = None;
      }

      let immediate = Immediate::create(
        builder,
        &ImmediateArgs {
          type_: FieldType::Blob,
          blob: blob,
          num: 0
        }
      );
      column_list.push( immediate );
    }

    let data = Some(builder.create_vector(column_list.as_slice()));
    let row = Row::create(builder, &RowArgs{
      len: 0,
      data: data
    });

    (false, Some(row))
  }
}

impl<'a> Display for RamTable<'a> {
  fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
    f.write_str(self.schema.schema().name())
  }
}

#[cfg(test)]
pub mod tests {
  use crate::utils::{create_memory_mapping, create_column, build_schema, build_immediate_string};
  use crate::ram::RamColumn;
  use crate::flat::{FieldDescription, FieldDescriptionArgs, TableSchemaArgs, CreateCommandArgs, PacketArgs, Command, get_root_as_packet, TableSchema, ImmediateArgs, ImmediateBuilder, Immediate, FieldType};
  use crate::flat;
  use crate::flat::FieldType::Blob;
  use flatbuffers::FlatBufferBuilder;
  use easybench::{bench_env, bench, bench_gen_env};
  use std::sync::atomic::{AtomicU32, Ordering};
  use std::thread;
  use std::sync::Arc;
  use std::thread::JoinHandle;
  use std::time::Instant;

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
}