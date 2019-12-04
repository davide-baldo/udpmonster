use crate::flat::{Immediate, Row, TableSchema, FieldDescription};
use crate::store::{Table, TableSchemaEx};
use crate::utils::create_memory_mapping;
use std::fmt::{Display, Formatter};
use std::io::Error;
use std::sync::atomic::{AtomicUsize, Ordering, AtomicIsize};
use std::cell::UnsafeCell;

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
  index: Box<&'static mut [usize]>,
  next_index_position: AtomicUsize,
  deleted_memory: u64,
}

impl ColumnDataBlob {
  fn create() -> Self {
    ColumnDataBlob {
      data: Box::new(create_memory_mapping()),
      index: Box::new(create_memory_mapping()),
      next_index_position: AtomicUsize::new(0),
      deleted_memory: 0,
    }
  }

  fn get(&self, index: u32) -> &[u8] {
    let blob = &self.data[self.index[index as usize] as usize..];
    let length: usize = (blob[0] as usize | blob[1] as usize >> 8) as usize;
    &blob[2..length+2]
  }


  fn insert(&mut self, index: u32, immediate: &Immediate) -> u32 {
    let blob = immediate.blob().unwrap();
    let length = blob.len() + 2;
    let last_index: usize = 0;
    loop {
      let val = self.next_index_position.load(Ordering::Relaxed);

      let last_index = self.next_index_position.compare_and_swap(
        val,
        val + length,
        Ordering::Relaxed,
      );

      if val == last_index {
        break;
      }
    }

    self.data[last_index+0] = (blob.len() & 0xFF) as u8;
    self.data[last_index+1] = (blob.len() << 8 & 0xFF) as u8;
    unsafe {
      libc::memcpy(
        self.data.as_mut_ptr().offset((last_index + 2) as isize) as *mut libc::c_void,
        blob.as_ptr() as *const libc::c_void,
        blob.len(),
      );
    }

    self.index[index as usize] = last_index as usize;

    index
  }
}

struct RamColumn {
  id: u8,
  type_: crate::flat::FieldType,
  size: u16,
  blob_data: Option<ColumnDataBlob>,
}

impl RamColumn {
  fn insert(&mut self, index: u32, immediate: &Immediate) -> u32 {
    self.blob_data.as_mut().unwrap().insert(index, immediate)
  }

  fn get_blob(&self, index: u32) -> &[u8] {
    self.blob_data.as_ref().unwrap().get(index)
  }

  fn create(field: &FieldDescription) -> Self {
    RamColumn {
      id: field.id(),
      type_: field.type_(),
      size: field.size_(),
      blob_data: Some(ColumnDataBlob::create()),
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

  #[test]
  fn test_simple_insert() {
    let mut builder = flatbuffers::FlatBufferBuilder::new_with_capacity(
      2048
    );

    let mut builder_immediate = flatbuffers::FlatBufferBuilder::new_with_capacity(
      2048
    );
    let immediate = build_immediate_string(
      &mut builder_immediate,
      "hello",
    );

    let schema = build_schema(&mut builder);
    let mut column = RamColumn::create(
      &schema.fields().get(0)
    );

    column.insert(0, &immediate);

    assert_eq!(column.get_blob(0), "hello".as_bytes());
  }
}