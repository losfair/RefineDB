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

pub struct PathWalker<'a> {
  node: &'a StorageNode,

  /// true if this is a dynamic key generated from the primary key of a set.
  is_dynamic_key: bool,

  key: KeyCow<'a>,
  keylink: Option<Arc<PathWalker<'a>>>,
}

#[derive(Clone)]
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
      is_dynamic_key: false,
      key: KeyCow::Borrowed(&export.key),
      keylink: None,
    }))
  }
}

impl<'a> PathWalker<'a> {
  pub fn enter_field(self: &Arc<Self>, field_name: &str) -> Result<Self> {
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
      let mut me = self;
      while let Some(link) = &me.keylink {
        if !link.is_dynamic_key && link.key.deref() == subspace_reference {
          return Ok(Self {
            node: link.node,
            is_dynamic_key: false,
            key: KeyCow::Borrowed(&node.key),
            keylink: Some(self.clone()),
          });
        }
        me = link;
      }
      return Err(PathWalkerError::ReferenceNodeNotFound.into());
    } else if node.flattened {
      Ok(Self {
        node,
        is_dynamic_key: false,
        key: self.key.clone(),
        keylink: self.keylink.clone(),
      })
    } else {
      Ok(Self {
        node,
        is_dynamic_key: false,
        key: KeyCow::Borrowed(&node.key),
        keylink: Some(self.clone()),
      })
    }
  }

  pub fn enter_set(self: &Arc<Self>, primary_key: &PrimitiveValue) -> Result<Self> {
    let set = &**self
      .node
      .set
      .as_ref()
      .ok_or_else(|| PathWalkerError::NotSet)?;
    let primary_key_bytes = primary_key.serialize_for_key_component();
    Ok(Self {
      node: set,
      is_dynamic_key: true,
      key: KeyCow::Owned(Arc::from(primary_key_bytes.as_slice())),
      keylink: Some(self.clone()),
    })
  }
}
