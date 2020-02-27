use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Instant;
use std::sync::atomic::{AtomicU32, Ordering};
use std::thread;
use crate::utils::{create_column,build_immediate_string,create_memory_mapping};

#[allow(dead_code, unused_imports)]
mod flat;
mod store;
mod commands;
mod udp;
mod ram;
mod utils;

fn main() {
  multi_thread_insert_and_get();
  //bench_direct_copy(400);
}

pub fn bench_direct_copy(mega_bytes: usize) {
  let memory: &mut [u8] = create_memory_mapping::<u8>();
  let mega_byte: [u8; 4096] = [0xAA; 4096];

  let mut offset : usize = 0;
  for i  in 0..mega_bytes*256 {
    unsafe {
      libc::memcpy(
        memory.as_mut_ptr().offset(offset as isize) as *mut libc::c_void,
        mega_byte.as_ptr() as *const libc::c_void,
        mega_byte.len(),
      );
    }
    offset += mega_byte.len();
  }
}

pub fn multi_thread_insert_and_get() {
  let column = Arc::new(create_column());
  let mut threads : Vec<JoinHandle<bool>> = Vec::new();
  let now = Instant::now();

  const NUMBER_OF_ROWS : u32 = 400000000;

  let write_index = Arc::new(AtomicU32::new(0));
  for _ in 0..8 {
    let column = column.clone();
    let write_index = write_index.clone();
    let result = thread::spawn(move || {
      let mut builder_immediate = flatbuffers::FlatBufferBuilder::new_with_capacity(
        2048
      );
      let immediate = build_immediate_string(
        &mut builder_immediate,
        "hello!",
      );
      loop {
        let index = write_index.fetch_add(1, Ordering::Relaxed);
        column.insert(index, &immediate);
        if index >= NUMBER_OF_ROWS {
          return true;
        }
      }
    });
    threads.push(result);
  }

  let read_index = Arc::new(AtomicU32::new(0));
  for _ in 0..0 {
    let column = column.clone();
    let read_index = read_index.clone();
    let result = thread::spawn( move || {
      loop {
        let index = read_index.fetch_add(1, Ordering::Relaxed);
        if index >= NUMBER_OF_ROWS {
          return true;
        }
        if let Some(x) = column.get_blob(NUMBER_OF_ROWS - index) {
          assert_eq!(x, "hello!".as_bytes())
        }
      }
    });
    threads.push(result);
  }

  for thread in threads {
    let join = thread.join();
    assert_eq!(join.is_err(), false);
    assert_eq!(join.is_ok(), true)
  }

 // println!("Size: {}",column.size());
}