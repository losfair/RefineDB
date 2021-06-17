use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub enum PackedValue {
  /// Primitive value.
  P(PrimitiveValue),

  /// Key-value map.
  M(BTreeMap<String, PackedValue>),

  /// Set.
  S(Vec<PackedValue>),
}

#[derive(Serialize, Deserialize)]
#[serde(untagged)]
pub enum PrimitiveValue {
  String(String),
  Bytes(Vec<u8>),
  Int64(i64),
  Double(f64),
}
