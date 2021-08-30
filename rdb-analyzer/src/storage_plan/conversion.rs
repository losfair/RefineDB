use std::convert::{TryFrom, TryInto};

use thiserror::Error;

use super::{StorageKey, StorageNode, StoragePlan};

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
      key: base64::encode(&that.key),
      flattened: that.flattened,
      subspace_reference: that.subspace_reference.map(|x| base64::encode(&x)),
      set: that.set.as_ref().map(|x| Box::new(Self::from(&**x))),
      children: that
        .children
        .iter()
        .map(|(k, v)| (k.clone(), Self::from(v)))
        .collect(),
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
      key: base64::decode(&that.key)
        .map_err(|_| StorageKeyConversionError::Base64Decode)
        .and_then(|x| {
          x.try_into()
            .map_err(|_| StorageKeyConversionError::Base64Decode)
        })?,
      flattened: that.flattened,
      subspace_reference: that
        .subspace_reference
        .as_ref()
        .map(|x| base64::decode(&x))
        .transpose()
        .map_err(|_| StorageKeyConversionError::Base64Decode)?
        .map(|x| {
          x.try_into()
            .map_err(|_| StorageKeyConversionError::Base64Decode)
        })
        .transpose()?,
      set: that
        .set
        .as_ref()
        .map(|x| Self::try_from(&**x).map(Box::new))
        .transpose()?,
      children: that
        .children
        .iter()
        .map(|(k, v)| Self::try_from(v).map(|v| (k.clone(), v)))
        .collect::<Result<_, StorageKeyConversionError>>()?,
    })
  }
}
