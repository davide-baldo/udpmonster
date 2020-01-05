use std::io::Error;
use flatbuffers::FlatBufferBuilder;
use crate::ram::{RamColumn};
use crate::flat;
use crate::flat::{Immediate,FieldType,Command,get_root_as_packet,ImmediateArgs,FieldDescription,FieldDescriptionArgs,TableSchemaArgs,CreateCommandArgs,PacketArgs,TableSchema};


/*
      //MAP_HUGETLB|MAP_HUGE_1GB 0x40000|0x78000000| do not work
      //MAP_ANONYMOUS|MAP_SHARED|MAP_NORESERVE|MAP_UNINITIALIZED
      0x1|0x20|0x4000|0x4000000,
      */
const MAP_UNINITIALIZED: i32 = 0x4000000;
const MAP_PRIVATE: i32 = 0x2;
const MAP_SHARED: i32 = 0x1;
const MAP_ANONYMOUS: i32 = 0x20;
const MAP_HUGETLB: i32 = 0x40000;
const MAP_HUGE_1GB: i32 = 0x78000000;
const MAP_NORESERVE: i32 = 0x4000;

const PROT_READ: i32 = 0x1;
const PROT_WRITE: i32 = 0x2;
const PROT_EXEC: i32 = 0x4;

pub fn create_memory_mapping<T>() -> &'static mut [T] {
  unsafe {
    let size: usize = 0x40000000000; //4TB
    let pointer = libc::mmap(
      0x0 as *mut libc::c_void,
      size,
      PROT_READ|PROT_WRITE,
      MAP_ANONYMOUS|MAP_PRIVATE|MAP_NORESERVE,
      -1,
      0x0,
    ) as *mut T;

    if pointer as usize == 0xffffffffffffffff as usize {
      panic!("mmap failed! {}", Error::last_os_error());
    }

    return std::slice::from_raw_parts_mut::<'static, T>(
      pointer,
      size
    )
  };
}

pub fn create_column() -> RamColumn {
  let mut builder = flatbuffers::FlatBufferBuilder::new_with_capacity(
    2048
  );
  let schema = build_schema(&mut builder);
  let mut column = RamColumn::create(
    &schema.fields().get(0)
  );
  column
}

pub fn build_schema<'a>(builder: &'a mut FlatBufferBuilder) -> TableSchema<'a> {
  build_create_table(builder);
  let data = builder.finished_data();
  let packet = get_root_as_packet(data);

  packet.command_as_create().unwrap().schema()
}

pub fn build_immediate_string<'a>(builder: &'a mut FlatBufferBuilder, string: &str) -> Immediate<'a> {
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

pub fn build_create_table(builder: &mut flatbuffers::FlatBufferBuilder) {
  let from = Some(builder.create_string("from"));
  let to = Some(builder.create_string("to"));
  let message = Some(builder.create_string("message"));

  let fields: [flatbuffers::WIPOffset<FieldDescription>; 3] = [
    flat::FieldDescription::create(
      builder,
      &FieldDescriptionArgs {
        id: 0x01,
        name: from,
        type_: FieldType::Blob,
        size_: 32,
      },
    ),
    flat::FieldDescription::create(
      builder,
      &FieldDescriptionArgs {
        id: 0x02,
        name: to,
        type_: FieldType::Blob,
        size_: 32,
      },
    ),
    flat::FieldDescription::create(
      builder,
      &FieldDescriptionArgs {
        id: 0x03,
        name: message,
        type_: FieldType::Blob,
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