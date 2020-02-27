use std::net::{UdpSocket, SocketAddrV4};
use std::thread;
use crate::flat::{get_root_as_packet, FilterValue, FilterType, PacketArgs, FilterArgs, Command, FieldDescription, TableSchemaArgs, CreateCommandArgs, FieldDescriptionArgs, InsertCommandArgs, Row, RowArgs, Immediate, ImmediateArgs, FieldType, QueryCommandArgs, Sort, CommandResponse, ResponsePacket};
use crate::{commands, flat};
use flatbuffers::{read_scalar, FlatBufferBuilder, WIPOffset};
use std::sync::{Arc, Mutex};
use crate::store::Store;
use crate::flat::FieldType::Blob;
use std::time::Duration;
use std::sync::atomic::{AtomicBool, Ordering};
use std::str::FromStr;

pub fn start_listening(threads: u16) -> Arc<AtomicBool> {
  let store = Arc::new(Mutex::new(Store::create()));
  let socket = UdpSocket::bind("0.0.0.0:8460").expect("port binding failed");
  let stopped = Arc::new(AtomicBool::new(false));

  for _x in 0..100 {
    let cloned_socket = socket.try_clone().expect("socket clone failed");
    let cloned_store = store.clone();
    let cloned_stopped = stopped.clone();

    thread::spawn(move || {
      UdpServer {
        socket: cloned_socket,
        store: cloned_store,
        stopped: cloned_stopped
      }.accept_packets();
    });
  }
  stopped
}

pub fn start_listening_for_test() -> Arc<AtomicBool> {
  start_listening(1)
}

struct UdpServer {
  socket: UdpSocket,
  store: Arc<Mutex<Store>>,
  stopped: Arc<AtomicBool>
}

impl UdpServer {
  fn accept_packets(&mut self) {
    let mut buffer_in: [u8; 64 * 1024] = [0; 64 * 1024];
    let mut builder = FlatBufferBuilder::new_with_capacity( 2048 );

    self.socket.set_read_timeout(Some(Duration::from_millis(250))).unwrap();
    loop {
      if self.stopped.load(Ordering::SeqCst) {
        return;
      }
      match self.socket.recv_from(&mut buffer_in) {
        Result::Ok((size, address)) => {

          let mut crc_calculated: u32;
          let mut crc_from_packet: u32;
          if false {
            crc_calculated = crc32c::crc32c(&buffer_in[0..size]);
            crc_from_packet = read_scalar::<u32>(&buffer_in[0..4]);
          } else {
            crc_calculated = 0;
            crc_from_packet = 0;
          }

          if crc_calculated == crc_from_packet {
            let packet = get_root_as_packet(&buffer_in[..size]);
            commands::exec(&mut self.socket, &address, packet, &mut self.store, &mut builder);
          } else {
            //send metrics!
          }
        }
        Err(err) => {
          //recv failed
        }
      }
    }
  }
}

fn build_create_table(builder: &mut flatbuffers::FlatBufferBuilder) {
  builder.reset();
  let from = Some(builder.create_string("from"));
  let to = Some(builder.create_string("to"));
  let message = Some(builder.create_string("message"));

  let fields: [flatbuffers::WIPOffset<FieldDescription>; 3] = [
    flat::FieldDescription::create(
      builder,
      &FieldDescriptionArgs {
        id: 0x03,
        name: message,
        type_: Blob,
        size_: 4096,
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
        id: 0x01,
        name: from,
        type_: Blob,
        size_: 32,
      },
    ),
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
  let command = flat::CreateCommand::create(
    builder,
    &CreateCommandArgs {
      schema: Some(schema),
      engine: engine,
    },
  );

  let packet = flat::Packet::create(
    builder,
    &PacketArgs {
      version: 1,
      request_id: 0,
      timeout: 1000,
      command_type: Command::Create,
      command: Some(command.as_union_value()),
    },
  );

  builder.finish(packet, None);
}

fn build_insert_rows(builder: &mut flatbuffers::FlatBufferBuilder) {
  builder.reset();

  let user_1 = builder.create_vector_direct(
    "user1".as_bytes()
  );
  let user_2 = builder.create_vector_direct(
    "user2".as_bytes()
  );
  let message_content = builder.create_vector_direct(
    "message".as_bytes()
  );

  let from = Immediate::create(
    builder,
    &ImmediateArgs {
      type_: FieldType::Blob,
      blob: Some(user_1),
      num: 0
    }
  );

  let to = Immediate::create(
    builder,
    &ImmediateArgs {
      type_: FieldType::Blob,
      blob: Some(user_2),
      num: 0
    }
  );

  let message = Immediate::create(
    builder,
    &ImmediateArgs {
      type_: FieldType::Blob,
      blob: Some(message_content),
      num: 0
    }
  );

  builder.start_vector::<WIPOffset<Immediate>>(3);
  builder.push(from);
  builder.push(to);
  builder.push(message);
  let data = builder.end_vector(3);

  let engine = Some(builder.create_string("ram"));
  let row = Row::create(
    builder,
    &RowArgs {
      len: 0,
      data: Some(data)
    }
  );

  let command = flat::InsertCommand::create(
    builder,
    &InsertCommandArgs {
      table_id: 1,
      row: Some(row)
    },
  );

  let packet = flat::Packet::create(
    builder,
    &PacketArgs {
      version: 1,
      request_id: 0,
      timeout: 1000,
      command_type: Command::Insert,
      command: Some(command.as_union_value()),
    },
  );

  builder.finish_minimal(packet);
}


fn build_simple_query(builder: &mut flatbuffers::FlatBufferBuilder) {
  builder.reset();

  let filter = flat::Filter::create(
    builder,
    &FilterArgs {
      type_: FilterType::None,
      left_value_type: FilterValue::NONE,
      left_value: Option::None,
      right_value_type: FilterValue::NONE,
      right_value: Option::None
    }
  );

  let command = flat::QueryCommand::create(
    builder,
    &QueryCommandArgs {
      table_id: 1,
      max_results: 3,
      sort: Sort::None,
      sort_field: 0,
      filter: Some(filter)
    },
  );

  let packet = flat::Packet::create(
    builder,
    &PacketArgs {
      version: 1,
      request_id: 0,
      timeout: 1000,
      command_type: Command::Query,
      command: Some(command.as_union_value()),
    },
  );

  builder.finish_minimal(packet);
}

#[test]
fn create_insert_query() {
  let stopped = start_listening_for_test();
  let mut in_buffer: [u8; 64*1024] = [0; 64*1024];

  let mut builder = flatbuffers::FlatBufferBuilder::new_with_capacity(
    2048
  );

  let address = SocketAddrV4::from_str("127.0.0.1:8460").unwrap();
  let socket = UdpSocket::bind("0.0.0.0:0" ).unwrap();

 // socket.set_read_timeout(Option::from(Duration::from_millis(500)));
  socket.set_write_timeout(Option::from(Duration::from_millis(500))).unwrap();

  {
    //create table & response
    builder.reset();
    build_create_table(&mut builder);
    assert_eq!(
      socket.send_to(builder.finished_data(), address).unwrap(),
      builder.finished_data().len()
    );

    let size = socket.recv(&mut in_buffer).unwrap();
    let response = flatbuffers::get_root::<ResponsePacket>(
      &in_buffer[0..size]
    );
    assert_eq!(
      response.response_type(),
      CommandResponse::Create
    );
  }

  {
    //insert few rows
    build_insert_rows(&mut builder);
    assert_eq!(
      socket.send_to(builder.finished_data(), address).unwrap(),
      builder.finished_data().len()
    );

    let size = socket.recv(&mut in_buffer).unwrap();
    let response = flatbuffers::get_root::<ResponsePacket>(&in_buffer[0..size]);
    assert_eq!(
      response.response_type(),
      CommandResponse::Insert
    );
  }

  {
    //query 3 lines without filters
    build_simple_query(&mut builder);
    assert_eq!(
      socket.send_to(builder.finished_data(), address).unwrap(),
      builder.finished_data().len()
    );

    let size = socket.recv(&mut in_buffer).unwrap();
    let response = flatbuffers::get_root::<ResponsePacket>(&in_buffer[0..size]);
    assert_eq!(
      response.response_type(),
      CommandResponse::Query
    );

    let query = response.response_as_query().unwrap();
    assert_eq!(query.rows().unwrap().len(), 3);

    let row = query.rows().unwrap().get(0);
    assert_eq!(row.data().len(), 3);

    let from = row.data().get(0);
    assert_eq!(
      String::from_utf8(from.blob().unwrap().into()).unwrap(),
      "user1"
    );

    let to = row.data().get(1);
    assert_eq!(
      String::from_utf8(to.blob().unwrap().into()).unwrap(),
      "user2"
    );

    let message = row.data().get(2);
    assert_eq!(
      String::from_utf8(message.blob().unwrap().into()).unwrap(),
      "message"
    );
  }

  stopped.store(true, Ordering::SeqCst);
}
