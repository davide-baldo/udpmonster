use std::task::{Context, Poll};
use std::pin::Pin;
use std::net::UdpSocket;
use std::io::Error;
use std::thread;
use crate::flat::{Packet, get_root_as_packet};
use crate::commands;
use flatbuffers::read_scalar;
use std::sync::{Arc, Mutex};
use std::borrow::BorrowMut;
use crate::store::Store;

pub fn start_listening() {
  let store = Arc::new(Mutex::new(Store::create()));
  let socket = UdpSocket::bind("0.0.0.0:8460").expect("port binding failed");


  for _x in 0..100 {
    let cloned_socket = socket.try_clone().expect("socket clone failed");
    let cloned_store = store.clone();

    thread::spawn(move || {
      UdpServer {
        socket: cloned_socket,
        store: cloned_store
      }.accept_packets()
    });
  }
}

struct UdpServer {
  socket: UdpSocket,
  store: Arc<Mutex<Store>>
}

impl UdpServer {
  fn accept_packets(&mut self) -> ! {
    let mut buffer_in: [u8; 64 * 1024] = [0; 64 * 1024];
    let mut buffer_out: [u8; 64 * 1024] = [0; 64 * 1024];
    loop {
      match self.socket.recv_from(&mut buffer_in) {
        Result::Ok((size, address)) => {
          let crc_calculated = crc32c::crc32c(&buffer_in[4..size]);
          let crc_from_packet = read_scalar::<u32>(&buffer_in[0..4]);

          if crc_calculated == crc_from_packet {
            let packet = get_root_as_packet(&buffer_in[..size]);
            commands::exec(&mut self.socket, &address, packet, &mut self.store, &mut buffer_out);
          } else {
            //send metrics!
          }
        }
        Err(_err) => {
          //recv failed
        }
      }
    }
  }
}
