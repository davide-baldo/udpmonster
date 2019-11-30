use crate::flat::{Packet, Command};
use crate::store::Store;
use std::sync::{Arc, Mutex};
use std::net::{UdpSocket, SocketAddr};
use std::borrow::BorrowMut;
use std::process::exit;

pub fn exec(
  socket: &mut UdpSocket,
  address: &SocketAddr,
  packet: Packet,
  store: &mut Arc<Mutex<Store>>,
  buffer_out: &mut [u8; 64 * 1024]
) {
  match packet.command_type() {
    Command::Create => {
      let create = packet.command_as_create().unwrap();
      let table = store.lock().unwrap().create_table(
        create.schema(),
        create.engine()
      );

      if table.is_some() {
        socket.send_to(&buffer_out[0..1], address);
      }
    },
    Command::Drop => {
      let drop = packet.command_as_drop().unwrap();
      store.lock().unwrap().drop_table(
        drop.id()
      );

      socket.send_to(&buffer_out[0..1], address);
    },
    Command::Insert => {
      let insert = packet.command_as_insert().unwrap();
      let result = store.lock().unwrap().get_table(insert.table_id());
      if let Some(table) = result {
        table.insert(&insert.row())
      }
    },
    Command::Delete => {},
    Command::Query => {},
    Command::Update => {},
    _=> {}
  }

}
