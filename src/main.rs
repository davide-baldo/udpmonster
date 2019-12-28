#[allow(dead_code, unused_imports)]
mod flat;
mod store;
mod commands;
mod udp;
mod ram;
mod utils;

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
    udp::start_listening(10);

    loop {
      sleep(Duration::new(10,0))
    }
  }
}

fn main() {
  udpmonster::start();
}
