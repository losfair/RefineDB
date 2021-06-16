use std::{collections::BTreeMap, sync::Arc};

use crate::schema::compile::FieldType;

pub mod planner;

pub type StorageKey = [u8; 16];

pub struct StoragePlan {
  pub nodes: BTreeMap<Arc<str>, StorageNode>,
}

#[derive(Clone)]
pub struct StorageNode {
  pub ty: FieldType,
  pub key: Option<StorageNodeKey>,
  pub subspace_reference: bool,
  pub children: BTreeMap<Arc<str>, StorageNode>,
}

#[derive(Clone)]
pub enum StorageNodeKey {
  Const(StorageKey),
  Set(Box<StorageNode>),
}
