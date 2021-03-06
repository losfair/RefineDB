use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, fmt::Display, io::Write, sync::Arc};

pub mod conversion;
pub mod planner;

#[cfg(test)]
mod planner_test;

pub type StorageKey = [u8; 12];

#[derive(Default, Clone, Serialize, Deserialize)]
pub struct StoragePlan<SK = StorageKey> {
  pub nodes: BTreeMap<Arc<str>, StorageNode<SK>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StorageNode<SK = StorageKey> {
  pub key: SK,
  pub flattened: bool,
  pub subspace_reference: Option<SK>,
  pub set: Option<Box<StorageNode<SK>>>,
  pub children: BTreeMap<Arc<str>, StorageNode<SK>>,
}

impl StoragePlan {
  pub fn serialize_compressed(&self) -> Result<Vec<u8>> {
    let serialized = rmp_serde::to_vec_named(self)?;
    let mut buf = Vec::new();
    snap::write::FrameEncoder::new(&mut buf).write_all(&serialized)?;
    Ok(buf)
  }
  pub fn deserialize_compressed(data: &[u8]) -> Result<Self> {
    Ok(rmp_serde::from_read(snap::read::FrameDecoder::new(data))?)
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
      " {}{}{}",
      hex::encode(&self.key.as_ref()),
      if let Some(x) = self.subspace_reference {
        format!(" subspace_reference({})", base64::encode(&x))
      } else {
        "".into()
      },
      if self.flattened { " flattened" } else { "" },
    )?;
    write!(f, "\n")?;

    match &self.set {
      Some(x) => {
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
