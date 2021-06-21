use std::{collections::BTreeMap, iter::FromIterator};

use byteorder::{BigEndian, ByteOrder};
use serde::{Deserialize, Serialize};
use smallvec::{smallvec, SmallVec};

use crate::schema::compile::PrimitiveType;

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
  pub fn get_type(&self) -> PrimitiveType {
    match self {
      PrimitiveValue::Bytes(_) => PrimitiveType::Bytes,
      PrimitiveValue::String(_) => PrimitiveType::String,
      PrimitiveValue::Int64(_) => PrimitiveType::Int64,
      PrimitiveValue::Double(_) => PrimitiveType::Double,
    }
  }

  /// https://activesphere.com/blog/2018/08/17/order-preserving-serialization
  pub fn serialize_for_key_component(&self) -> SmallVec<[u8; 9]> {
    match self {
      PrimitiveValue::Bytes(x) => SmallVec::from_iter(
        std::iter::once(0x01u8)
          .chain(
            x.iter()
              .map(|&x| -> SmallVec<[u8; 2]> {
                if x == 0 {
                  smallvec![0x00, 0xff]
                } else {
                  smallvec![x]
                }
              })
              .flatten(),
          )
          .chain([0x00u8, 0x00u8].iter().copied()),
      ),
      PrimitiveValue::String(x) => SmallVec::from_iter(
        std::iter::once(0x02u8)
          .chain(x.as_bytes().iter().copied())
          .chain(std::iter::once(0x00u8)),
      ),
      PrimitiveValue::Int64(x) => {
        // Flip the top bit for order preservation.
        let x = (*x as u64) ^ TOP_BIT;

        let mut buf = smallvec![0u8; 9];
        buf[0] = 0x03;
        BigEndian::write_u64(&mut buf[1..], x);
        buf
      }
      PrimitiveValue::Double(x) => {
        let x = x.to_bits();

        let x = if x & TOP_BIT != 0 { !x } else { x ^ TOP_BIT };

        let mut buf = smallvec![0u8; 9];
        buf[0] = 0x04;
        BigEndian::write_u64(&mut buf[1..], x);
        buf
      }
    }
  }

  #[cfg(test)]
  pub fn example_value_for_type(ty: PrimitiveType) -> Self {
    match ty {
      PrimitiveType::Bytes => Self::Bytes(vec![0xbe, 0xef]),
      PrimitiveType::String => Self::String("hello".into()),
      PrimitiveType::Int64 => Self::Int64(42),
      PrimitiveType::Double => Self::Double(3.14),
    }
  }

  pub fn default_value_for_type(ty: PrimitiveType) -> Self {
    match ty {
      PrimitiveType::Bytes => Self::Bytes(vec![]),
      PrimitiveType::String => Self::String("".into()),
      PrimitiveType::Int64 => Self::Int64(0),
      PrimitiveType::Double => Self::Double(0.0),
    }
  }
}
