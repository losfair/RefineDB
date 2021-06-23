use std::{ops::Deref, sync::Arc};

use anyhow::Result;

use crate::storage_plan::{StorageNode, StoragePlan};
use thiserror::Error;

use super::value::PrimitiveValue;

#[derive(Error, Debug)]
pub enum PathWalkerError {
  #[error("field not found: `{0}`")]
  FieldNotFound(String),

  #[error("enter_field called on set")]
  EnterFieldOnSet,

  #[error("cannot find referenced node for subspace reference")]
  ReferenceNodeNotFound,

  #[error("enter_set called on a non-set node")]
  NotSet,
}

#[derive(Debug)]
pub struct PathWalker<'a> {
  /// The "actual" storage node, with subspace references resolved.
  node: &'a StorageNode,

  /// The current key component.
  key: KeyCow<'a>,

  /// Link to the parent node.
  link: Option<Arc<PathWalker<'a>>>,

  /// Whether this node should be flattened.
  ///
  /// False if:
  /// - `node.flattened == false`.
  /// - This is a subspace reference.
  ///
  /// True otherwise.
  should_flatten: bool,
}

#[derive(Clone, Debug)]
enum KeyCow<'a> {
  Borrowed(&'a [u8]),
  Owned(Arc<[u8]>),
}

impl<'a> Deref for KeyCow<'a> {
  type Target = [u8];
  fn deref(&self) -> &Self::Target {
    match self {
      KeyCow::Borrowed(x) => *x,
      KeyCow::Owned(x) => &**x,
    }
  }
}

impl<'a> PathWalker<'a> {
  pub fn from_export(plan: &'a StoragePlan, export_name: &str) -> Result<Arc<Self>> {
    let export = plan
      .nodes
      .get(export_name)
      .ok_or_else(|| PathWalkerError::FieldNotFound(export_name.to_string()))?;

    Ok(Arc::new(Self {
      node: export,
      key: KeyCow::Borrowed(&export.key),
      link: None,
      should_flatten: export.flattened,
    }))
  }
}

impl<'a> PartialEq for PathWalker<'a> {
  fn eq(&self, other: &Self) -> bool {
    self.generate_key_raw() == other.generate_key_raw()
  }
}

impl<'a> PathWalker<'a> {
  fn generate_key_raw(&self) -> Vec<&[u8]> {
    let mut components: Vec<&[u8]> = vec![];

    // The leaf node should always have its key component appended
    components.push(&self.key);

    let mut link = self.link.as_ref();

    while let Some(x) = link {
      if !x.should_flatten {
        components.push(&x.key);
      }
      link = x.link.as_ref();
    }
    components.reverse();
    components
  }

  pub fn node(&self) -> &'a StorageNode {
    self.node
  }

  pub fn generate_key(&self) -> Vec<u8> {
    let components = self.generate_key_raw();
    let len = components.iter().fold(0, |a, b| a + b.len());
    let mut key = Vec::with_capacity(len);
    for c in components.iter() {
      key.extend_from_slice(*c);
    }
    assert_eq!(key.len(), len);
    key
  }

  pub fn generate_key_pretty(&self) -> String {
    return self
      .generate_key_raw()
      .iter()
      .map(|x| format!("[{}]", base64::encode(x)))
      .collect::<Vec<_>>()
      .join(" ");
  }

  pub fn enter_field(self: &Arc<Self>, field_name: &str) -> Result<Arc<Self>> {
    // This check is not necessary for correctness but let's optimize our error message
    if self.node.set.is_some() {
      return Err(PathWalkerError::EnterFieldOnSet.into());
    }

    let node = self
      .node
      .children
      .get(field_name)
      .ok_or_else(|| PathWalkerError::FieldNotFound(field_name.to_string()))?;

    if let Some(subspace_reference) = node.subspace_reference {
      // Walk up the list
      let mut me = Some(self);
      while let Some(link) = me {
        // Here we use `link.node.key` instead of `link.key` to avoid conflicting with set keys.
        if link.node.key == subspace_reference {
          // Use the referenced node, with our own key.
          // And do not flatten.
          return Ok(Arc::new(Self {
            node: link.node,
            key: KeyCow::Borrowed(&node.key),
            link: Some(self.clone()),
            should_flatten: false,
          }));
        }
        me = link.link.as_ref();
      }
      return Err(PathWalkerError::ReferenceNodeNotFound.into());
    } else {
      Ok(Arc::new(Self {
        node,
        key: KeyCow::Borrowed(&node.key),
        link: Some(self.clone()),
        should_flatten: node.flattened,
      }))
    }
  }

  pub fn set_fast_scan_prefix(&self) -> Result<Vec<u8>> {
    self
      .node
      .set
      .as_ref()
      .ok_or_else(|| PathWalkerError::NotSet)?;

    let mut key = self.generate_key();
    key.push(0x01u8);
    Ok(key)
  }

  pub fn set_data_prefix(&self) -> Result<Vec<u8>> {
    self
      .node
      .set
      .as_ref()
      .ok_or_else(|| PathWalkerError::NotSet)?;

    let mut key = self.generate_key();
    key.push(0x00u8);
    Ok(key)
  }

  pub fn enter_set_raw(self: &Arc<Self>, primary_key: &[u8]) -> Result<Arc<Self>> {
    let set = &**self
      .node
      .set
      .as_ref()
      .ok_or_else(|| PathWalkerError::NotSet)?;

    // 0x00 - data
    // 0x01 - key only
    // 0x02 - index
    let mut dynamic_key_bytes = vec![0x00u8];
    dynamic_key_bytes.extend_from_slice(primary_key);
    dynamic_key_bytes.push(0x00u8);

    let dynamic_key = KeyCow::Owned(Arc::from(dynamic_key_bytes.as_slice()));

    // The set key.
    let intermediate = Arc::new(Self {
      node: set,
      key: dynamic_key.clone(),
      link: Some(self.clone()),
      should_flatten: false,
    });

    // And the table key.
    Ok(Arc::new(Self {
      node: set,
      key: KeyCow::Borrowed(&set.key),
      link: Some(intermediate),
      should_flatten: true,
    }))
  }

  pub fn enter_set(self: &Arc<Self>, primary_key: &PrimitiveValue) -> Result<Arc<Self>> {
    self.enter_set_raw(&primary_key.serialize_for_key_component())
  }
}
