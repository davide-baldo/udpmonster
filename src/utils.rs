use std::mem;
use std::io::Error;
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