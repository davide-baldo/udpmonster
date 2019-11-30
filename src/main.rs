#[allow(dead_code, unused_imports)]
mod flat;
mod store;
mod commands;
mod udp;

mod udpmonster {
  use crate::flat;
  use std::collections::HashMap;
  use std::sync::Arc;
  use crate::flat::FieldType::Blob;
  use std::fmt::{Display, Formatter, Error};
  use std::marker::PhantomData;
  use crate::store::Store;
  use crate::udp;
  use std::thread::sleep;
  use std::time::Duration;

  pub fn start() {
    udp::start_listening();

    loop {
      sleep(Duration::new(10,0))
    }
  }

  /*
    let mut builder = flatbuffers::FlatBufferBuilder::new_with_capacity(
      2048
    );
    build_create_table_packet(&mut builder);

    let data = builder.finished_data();
    //print!("Data:\n{}", base64::encode(data));

    let packet = get_root_as_packet(data);
    let mut store = Store::create();

    let table = store.create_table(packet.command_as_create().unwrap().schema(), "ram" );

    if let Some(u) = table {
      print!("Table: {}", u);
    }


  fn build_create_table_packet(builder: &mut flatbuffers::FlatBufferBuilder) {
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
*/
}

fn main() {
  udpmonster::start();
}
