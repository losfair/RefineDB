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
      key: KeyCow::Borrowed(&export.key),
      link: None,
      should_flatten: export.flattened,
    }))
  }
}

impl<'a> PathWalker<'a> {
  pub fn generate_key(&self) -> Vec<u8> {
    let mut components: Vec<&[u8]> = vec![];
    let mut len = 0usize;

    // The leaf node should always have its key component appended
    components.push(&self.key);
    len += self.key.len();

    let mut link = self.link.as_ref();

    while let Some(x) = link {
      if !x.should_flatten {
        components.push(&x.key);
        len += x.key.len();
      }
      link = x.link.as_ref();
    }
    let mut key = Vec::with_capacity(len);
    for c in components.iter().rev() {
      key.extend_from_slice(*c);
    }
    assert_eq!(key.len(), len);
    key
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
      let mut me = self;
      while let Some(link) = &me.link {
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
        me = link;
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

  pub fn enter_set(self: &Arc<Self>, primary_key: &PrimitiveValue) -> Result<Arc<Self>> {
    let set = &**self
      .node
      .set
      .as_ref()
      .ok_or_else(|| PathWalkerError::NotSet)?;
    let primary_key_bytes = primary_key.serialize_for_key_component();

    let dynamic_key = KeyCow::Owned(Arc::from(primary_key_bytes.as_slice()));

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
}
