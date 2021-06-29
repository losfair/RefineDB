use std::{
  collections::HashSet,
  hash::Hash,
  time::{SystemTime, UNIX_EPOCH},
};

use byteorder::{BigEndian, ByteOrder};
use rand::RngCore;

pub fn first_duplicate<T>(iter: T) -> Option<T::Item>
where
  T: IntoIterator,
  T::Item: Eq + Hash,
{
  let mut uniq = HashSet::new();
  for x in iter {
    if uniq.contains(&x) {
      return Some(x);
    }
    uniq.insert(x);
  }
  None
}

pub fn rand_kn_stateless<const N: usize>() -> [u8; N] {
  assert!(N > 6);

  let now = SystemTime::now()
    .duration_since(UNIX_EPOCH)
    .unwrap()
    .as_millis() as u64;
  let mut timebuf = [0u8; 8];
  BigEndian::write_u64(&mut timebuf, now);

  assert_eq!(timebuf[0], 0);
  assert_eq!(timebuf[1], 0);

  let mut ret = [0u8; N];
  ret[..6].copy_from_slice(&timebuf[2..]);
  rand::thread_rng().fill_bytes(&mut ret[6..]);
  ret
}
