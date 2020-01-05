use crate::flat::{Packet, Command, QueryCommand, ResponsePacket, ResponsePacketArgs, QueryRowResponse, CommandResponse, QueryRowResponseArgs, EmptyCommandResponse, EmptyCommandResponseArgs};
use crate::store::{Store, Table};
use std::sync::{Arc, Mutex};
use std::net::{UdpSocket, SocketAddr};
use flatbuffers::{FlatBufferBuilder};

fn build_insert_response(builder: &mut FlatBufferBuilder) {
  builder.reset();
  let response = EmptyCommandResponse::create(
    builder,
    &EmptyCommandResponseArgs{}
  ).as_union_value();

  let root = ResponsePacket::create(
    builder,
    &ResponsePacketArgs {
      crc: 0,
      version: 0,
      response_type: CommandResponse::Insert,
      response: Some(response)
    },
  );
  builder.finish_minimal(root);
}

fn build_create_table_response(builder: &mut FlatBufferBuilder) {
  builder.reset();
  let response = EmptyCommandResponse::create(
    builder,
    &EmptyCommandResponseArgs{}
  ).as_union_value();

  let root = ResponsePacket::create(
    builder,
    &ResponsePacketArgs {
      crc: 0,
      version: 0,
      response_type: CommandResponse::Create,
      response: Some(response)
    },
  );
  builder.finish_minimal(root);
}

pub fn exec(
  socket: &mut UdpSocket,
  address: &SocketAddr,
  packet: Packet,
  store: &mut Arc<Mutex<Store>>,
  builder: &mut FlatBufferBuilder
) {
  match packet.command_type() {
    Command::Create => {
      let create = packet.command_as_create().unwrap();
      let table = store.lock().unwrap().create_table(
        create.schema(),
        create.engine()
      );

      if table.is_some() {
        build_create_table_response(builder);
        socket.send_to(&builder.finished_data(), address);
      }
    },
    Command::Drop => {
      let drop = packet.command_as_drop().unwrap();
      store.lock().unwrap().drop_table(
        drop.id()
      );

      socket.send_to(&builder.finished_data(), address);
    },
    Command::Insert => {
      let insert = packet.command_as_insert().unwrap();
      let result = store.lock().unwrap().get_table(insert.table_id());
      if let Some(table) = result {
        table.insert(&insert.row());
        build_insert_response(builder);
        socket.send_to(&builder.finished_data(), address);
      }
    },
    Command::Delete => {},
    Command::Query => {
      let query = packet.command_as_query().unwrap();

      if let Some(table) = store.lock().unwrap().get_table(query.table_id()) {
        builder.reset();
        let mut iterator = RowIterator::new(&table, &query);

        while iterator.has_next() {
          let index = iterator.next();

          if let Some(row) = table.copy_row(index, builder) {
            let response;
            let rows = builder.create_vector(&[row]);
            response = QueryRowResponse::create(builder, &QueryRowResponseArgs{
              index: index,
              rows: Some(rows),
              last: !iterator.has_next()
            }).as_union_value();

            send_row_packet(socket, address, builder, response);
          }
        }
      }
    },
    Command::Update => {},
    _=> {}
  }

  fn send_row_packet(
    socket: &mut UdpSocket,
    address: &SocketAddr,
    builder: &mut FlatBufferBuilder,
    response: flatbuffers::WIPOffset<flatbuffers::UnionWIPOffset>
  ) {
    let response = ResponsePacket::create(
      builder,
      &ResponsePacketArgs {
        crc: 0x0,
        version: 0x0,
        response_type: CommandResponse::Query,
        response: Some(response)
      }
    );

    builder.finish_minimal(response);
    socket.send_to(builder.finished_data(), address).expect("send_to failed");
  }

  struct RowIterator<'a> {
    index: u32,
    table: Arc<dyn Table<'a>>
  }

  impl<'a> RowIterator<'a> {
    fn new(table : &Arc<dyn Table<'a>>, query: &QueryCommand) -> RowIterator<'a>{
      RowIterator {
        index: 0,
        table: table.clone()
      }
    }

    pub fn has_next(&mut self) -> bool {
      return self.index < 10;
    }

    pub fn next(&mut self) -> u32 {
      let current = self.index;
      self.index += 1;
      current
    }
  }
}
