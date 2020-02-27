use crate::flat::{Packet, Command, Row, QueryCommand, ResponsePacket, ResponsePacketArgs, QueryRowResponse, CommandResponse, QueryRowResponseArgs, EmptyCommandResponse, EmptyCommandResponseArgs, FilterType};
use crate::store::{Store, Table};
use std::sync::{Arc, Mutex};
use std::net::{UdpSocket, SocketAddr};
use flatbuffers::{FlatBufferBuilder, WIPOffset, UOffsetT, Vector};
use std::cell::RefCell;
use std::borrow::BorrowMut;
use std::marker::PhantomData;

fn build_insert_response(builder: &mut FlatBufferBuilder) {
  builder.reset();
  let response = EmptyCommandResponse::create(
    builder,
    &EmptyCommandResponseArgs{}
  ).as_union_value();

  let root = ResponsePacket::create(
    builder,
    &ResponsePacketArgs {
      version: 0,
      request_id: 0,
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
      version: 0,
      request_id: 0,
      response_type: CommandResponse::Create,
      response: Some(response)
    },
  );
  builder.finish_minimal(root);
}

pub fn exec<'a, 'fbb>(
  socket: &mut UdpSocket,
  address: & SocketAddr,
  packet: Packet,
  store: &mut Arc<Mutex<Store>>,
  builder: &'a mut FlatBufferBuilder<'fbb>
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
        let _ = socket.send_to(&builder.finished_data(), address);
      }
    },
    Command::Drop => {
      let drop = packet.command_as_drop().unwrap();
      store.lock().unwrap().drop_table(
        drop.id()
      );

      let _ = socket.send_to(&builder.finished_data(), address);
    },
    Command::Insert => {
      let insert = packet.command_as_insert().unwrap();
      let result = store.lock().unwrap().get_table(insert.table_id());
      if let Some(table) = result {
        table.insert(&insert.row());
        build_insert_response(builder);
        let _ = socket.send_to(&builder.finished_data(), address);
      }
    },
    Command::Delete => {},
    Command::Query => {
      let query = packet.command_as_query().unwrap();

      if let Some(table) = store.lock().unwrap().get_table(query.table_id()) {
        builder.reset();
        let mut iterator = RowIterator::new(&table, &query, 62*1024);
        let mut rows_vec : Vec<WIPOffset<Row>> = Vec::new();

        let empty = true;

        while iterator.has_next(builder) {
          if iterator.is_buffer_full() {
            let response;
            let rows = builder.create_vector(&rows_vec);
            response = QueryRowResponse::create(builder, &QueryRowResponseArgs {
              index: 0,
              rows: Some(rows),
              last: false //true only when all the iterator is actually _consumed_
            }).as_union_value();

            send_row_packet(socket, address, builder, response);
            builder.reset();
            rows_vec.clear();
          } else {
            let (index, row) = iterator.next(builder);
            rows_vec.push(row);
          }
        }

        let rows = builder.create_vector(&rows_vec);
        let response = QueryRowResponse::create(builder, &QueryRowResponseArgs {
          index: 0,
          rows: Some(rows),
          last: true
        }).as_union_value();

        send_row_packet(socket, address, builder, response);
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
        version: 0x0,
        request_id: 0,
        response_type: CommandResponse::Query,
        response: Some(response)
      }
    );

    builder.finish_minimal(response);
    socket.send_to(builder.finished_data(), address).expect("send_to failed");
  }

  struct RowIterator<'a,'b> {
    index: u32,
    table: Arc<dyn Table<'a>>,
    query: &'b QueryCommand<'b>,
    results_left: u16,
    current_row: Option<UOffsetT>,
    current_row_index: u32,
    max_size: u16,
    buffer_full: bool
  }

  impl<'a, 'b> RowIterator<'a, 'b> {
    fn new(
      table : &Arc<dyn Table<'a>>,
      query: &'b QueryCommand<'b>,
      max_size: u16
    ) -> RowIterator<'a, 'b>{
      RowIterator {
        index: 0,
        table: table.clone(),
        query: query,
        results_left: query.max_results(),
        current_row: Option::None,
        current_row_index: 0,
        max_size: max_size,
        buffer_full: false
      }
    }

    pub fn is_buffer_full(&self) -> bool {
      self.buffer_full
    }

    pub fn has_next(&mut self, builder: &mut FlatBufferBuilder) -> bool {
      if self.buffer_full {
        self.buffer_full = false;
      }

      if self.current_row.is_some() {
        return true;
      }

      if /*self.index > self.table.rows() || */ self.results_left == 0 {
        return false;
      }

      return {
        let (buffer_full, row) = self.table.copy_row(self.index, builder, self.max_size);

        if buffer_full {
          self.buffer_full = true;
          true
        } else {
          if row.is_some() {
            let valid: bool;

            match self.query.filter().type_() {
              FilterType::None => {
                valid = true;
              }
              _ => {
                panic!("not implemented!")
              }
            }

            if valid {
              self.current_row = Option::Some(row.unwrap().value());
              self.current_row_index = self.index;
              true
            } else {
              self.has_next(builder)
            }
          } else {
            self.index += 1;
            self.has_next(builder)
          }
        }
      }
    }

    pub fn next<'c>(&mut self, builder: &mut FlatBufferBuilder<'c>) -> (u32,WIPOffset<Row<'c>>) {
      self.index += 1;
      self.results_left -= 1;

      if let Some(row) = self.current_row {
        self.current_row = None;
        (self.current_row_index, WIPOffset::new(row))
      } else {
        panic!("invalid next()");
      }
    }
  }
}
