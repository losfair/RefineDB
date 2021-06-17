use std::collections::BTreeMap;

use byteorder::{BigEndian, ByteOrder};
use serde::{Deserialize, Serialize};
use smallvec::{smallvec, SmallVec};

#[derive(Serialize, Deserialize)]
pub enum PackedValue {
  /// Primitive value.
  P(PrimitiveValue),

  /// Key-value map.
  M(BTreeMap<String, PackedValue>),

  /// Set.
  S(Vec<PackedValue>),
}

#[derive(Clone, Serialize, Deserialize, Debug)]
#[serde(untagged)]
pub enum PrimitiveValue {
  String(String),
  Bytes(Vec<u8>),
  Int64(i64),
  Double(f64),
}

const TOP_BIT: u64 = 1u64 << 63;

impl PrimitiveValue {
  /// https://activesphere.com/blog/2018/08/17/order-preserving-serialization
  pub fn serialize_raw(&self) -> SmallVec<[u8; 8]> {
    match self {
      PrimitiveValue::Bytes(x) => SmallVec::from_slice(x),
      PrimitiveValue::String(x) => SmallVec::from_slice(x.as_bytes()),
      PrimitiveValue::Int64(x) => {
        // Flip the top bit for order preservation.
        let x = (*x as u64) ^ TOP_BIT;

        let mut buf = smallvec![0u8; 8];
        BigEndian::write_u64(&mut buf, x);
        buf
      }
      PrimitiveValue::Double(x) => {
        let x = x.to_bits();

        let x = if x & TOP_BIT != 0 { !x } else { x ^ TOP_BIT };

        let mut buf = smallvec![0u8; 8];
        BigEndian::write_u64(&mut buf, x);
        buf
      }
    }
  }
}
