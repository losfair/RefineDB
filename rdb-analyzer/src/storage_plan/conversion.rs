use std::convert::{TryFrom, TryInto};

use thiserror::Error;

use super::{StorageKey, StorageNode, StorageNodeKey, StoragePlan};

#[derive(Error, Debug)]
pub enum StorageKeyConversionError {
  #[error("cannot decode base64-encoded storage key")]
  Base64Decode,
}

impl From<&StoragePlan<StorageKey>> for StoragePlan<String> {
  fn from(that: &StoragePlan<StorageKey>) -> Self {
    Self {
      nodes: that
        .nodes
        .iter()
        .map(|(k, v)| (k.clone(), StorageNode::<String>::from(v)))
        .collect(),
    }
  }
}

impl From<&StorageNode<StorageKey>> for StorageNode<String> {
  fn from(that: &StorageNode<StorageKey>) -> Self {
    Self {
      key: that.key.as_ref().map(StorageNodeKey::<String>::from),
      subspace_reference: that.subspace_reference,
      packed: that.packed,
      children: that
        .children
        .iter()
        .map(|(k, v)| (k.clone(), Self::from(v)))
        .collect(),
    }
  }
}

impl From<&StorageNodeKey<StorageKey>> for StorageNodeKey<String> {
  fn from(that: &StorageNodeKey<StorageKey>) -> Self {
    match that {
      StorageNodeKey::Const(x) => Self::Const(base64::encode(x)),
      StorageNodeKey::Set(x) => Self::Set(Box::new(StorageNode::<String>::from(&**x))),
    }
  }
}

impl TryFrom<&StoragePlan<String>> for StoragePlan<StorageKey> {
  type Error = StorageKeyConversionError;

  fn try_from(that: &StoragePlan<String>) -> Result<Self, Self::Error> {
    Ok(Self {
      nodes: that
        .nodes
        .iter()
        .map(|(k, v)| StorageNode::<StorageKey>::try_from(v).map(|v| (k.clone(), v)))
        .collect::<Result<_, StorageKeyConversionError>>()?,
    })
  }
}

impl TryFrom<&StorageNode<String>> for StorageNode<StorageKey> {
  type Error = StorageKeyConversionError;

  fn try_from(that: &StorageNode<String>) -> Result<Self, Self::Error> {
    Ok(Self {
      key: that
        .key
        .as_ref()
        .map(StorageNodeKey::<StorageKey>::try_from)
        .transpose()?,
      subspace_reference: that.subspace_reference,
      packed: that.packed,
      children: that
        .children
        .iter()
        .map(|(k, v)| Self::try_from(v).map(|v| (k.clone(), v)))
        .collect::<Result<_, StorageKeyConversionError>>()?,
    })
  }
}

impl TryFrom<&StorageNodeKey<String>> for StorageNodeKey<StorageKey> {
  type Error = StorageKeyConversionError;

  fn try_from(that: &StorageNodeKey<String>) -> Result<Self, Self::Error> {
    Ok(match that {
      StorageNodeKey::Const(x) => Self::Const(
        base64::decode(x)
          .map_err(|_| StorageKeyConversionError::Base64Decode)?
          .try_into()
          .map_err(|_| StorageKeyConversionError::Base64Decode)?,
      ),
      StorageNodeKey::Set(x) => Self::Set(Box::new(StorageNode::<StorageKey>::try_from(&**x)?)),
    })
  }
}
