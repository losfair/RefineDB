use std::{collections::BTreeMap, fmt::Display, sync::Arc};

use crate::schema::compile::FieldType;
use serde::{Deserialize, Serialize};

pub mod planner;

#[cfg(test)]
mod planner_test;

pub type StorageKey = [u8; 16];

#[derive(Serialize, Deserialize)]
pub struct StoragePlan {
  pub nodes: BTreeMap<Arc<str>, StorageNode>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct StorageNode {
  pub ty: FieldType,
  pub key: Option<StorageNodeKey>,
  pub subspace_reference: bool,
  pub packed: bool,
  pub children: BTreeMap<Arc<str>, StorageNode>,
}

#[derive(Clone, Serialize, Deserialize)]
pub enum StorageNodeKey {
  Const(StorageKey),
  Set(Box<StorageNode>),
}

impl Display for StorageNodeKey {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      StorageNodeKey::Const(x) => write!(f, "{}", hex::encode(x)),
      StorageNodeKey::Set(_) => write!(f, "set_key"),
    }
  }
}

impl Display for StoragePlan {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    for (node_name, node) in &self.nodes {
      write!(f, "top-level node: {}{}", node_name, node)?;
    }
    Ok(())
  }
}

impl StorageNode {
  fn display_fmt(&self, indent: usize, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(
      f,
      " {} {}{}{}",
      self.ty,
      self
        .key
        .as_ref()
        .map(|x| format!("{}", x))
        .unwrap_or_else(|| "no_key".to_string()),
      if self.subspace_reference {
        " subspace_reference"
      } else {
        ""
      },
      if self.packed { " packed" } else { "" },
    )?;
    write!(f, "\n")?;

    match &self.key {
      Some(StorageNodeKey::Set(x)) => {
        for _ in 0..indent + 1 {
          write!(f, ".")?;
        }
        write!(f, "<set_member>")?;
        x.display_fmt(indent + 1, f)?;
      }
      _ => {
        for (child_name, child_node) in &self.children {
          for _ in 0..indent + 1 {
            write!(f, ".")?;
          }
          write!(f, "{}", child_name)?;
          child_node.display_fmt(indent + 1, f)?;
        }
      }
    }
    Ok(())
  }
}

impl Display for StorageNode {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    self.display_fmt(0, f)
  }
}
