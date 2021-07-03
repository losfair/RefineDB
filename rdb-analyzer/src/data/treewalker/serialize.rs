use std::{collections::BTreeMap, sync::Arc};

use anyhow::Result;

use crate::{
  data::{
    treewalker::vm_value::{VmListValue, VmMapValue},
    value::PrimitiveValue,
  },
  schema::compile::PrimitiveType,
};

use super::vm_value::{VmType, VmValue};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum SerializeError {
  #[error("unserializable value")]
  Unserializable,

  #[error("type mismatch")]
  TypeMismatch,

  #[error("type mismatch during unwrapping")]
  UnwrapTypeMismatch,

  #[error("unexpected null value")]
  UnexpectedNullValue,

  #[error("missing required field: `{0}`")]
  MissingRequiredField(String),
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
pub enum SerializedVmValue {
  String(String),
  Bool(bool),
  Bytes(Vec<u8>),
  Int64(i64),
  Double(f64),
  Null(Option<Never>),
  Tagged(TaggedVmValue),
}

#[derive(Serialize, Deserialize, Debug)]
pub enum TaggedVmValue {
  M(BTreeMap<String, SerializedVmValue>),
  L(Vec<SerializedVmValue>),
}

#[derive(Default, Debug, Clone)]
pub struct VmValueEncodeConfig {
  pub enable_bytes: bool,
  pub enable_int64: bool,
  pub enable_double: bool,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Never {}

impl SerializedVmValue {
  pub fn try_unwrap_bool(&self) -> Result<bool> {
    match self {
      Self::Bool(x) => Ok(*x),
      _ => Err(SerializeError::UnwrapTypeMismatch.into()),
    }
  }
  pub fn try_unwrap_list(&self) -> Result<&Vec<SerializedVmValue>> {
    match self {
      Self::Tagged(TaggedVmValue::L(x)) => Ok(x),
      _ => Err(SerializeError::UnwrapTypeMismatch.into()),
    }
  }
  pub fn check_nonnull(&self) -> Result<()> {
    match self {
      Self::Null(_) => Err(SerializeError::UnexpectedNullValue.into()),
      _ => Ok(()),
    }
  }
  pub fn try_unwrap_map(
    &self,
    required_fields: &[&str],
  ) -> Result<&BTreeMap<String, SerializedVmValue>> {
    match self {
      Self::Tagged(TaggedVmValue::M(x)) => {
        for f in required_fields {
          if !x.contains_key(*f) {
            return Err(SerializeError::MissingRequiredField(f.to_string()).into());
          }
        }
        Ok(x)
      }
      _ => Err(SerializeError::UnwrapTypeMismatch.into()),
    }
  }
  pub fn try_unwrap_string(&self) -> Result<&String> {
    match self {
      Self::String(x) => Ok(x),
      _ => Err(SerializeError::UnwrapTypeMismatch.into()),
    }
  }
  pub fn try_unwrap_bytes(&self) -> Result<&Vec<u8>> {
    match self {
      Self::Bytes(x) => Ok(x),
      _ => Err(SerializeError::UnwrapTypeMismatch.into()),
    }
  }
  pub fn try_unwrap_int64(&self) -> Result<i64> {
    match self {
      Self::Int64(x) => Ok(*x),
      _ => Err(SerializeError::UnwrapTypeMismatch.into()),
    }
  }

  pub fn encode(v: &VmValue, config: &VmValueEncodeConfig) -> Result<Self> {
    match v {
      VmValue::Map(x) => Ok(Self::Tagged(TaggedVmValue::M(
        x.elements
          .iter()
          .map(|(k, v)| Self::encode(&**v, config).map(|x| (k.to_string(), x)))
          .collect::<Result<_>>()?,
      ))),
      VmValue::Null(_) => Ok(Self::Null(None)),
      VmValue::Bool(x) => Ok(Self::Bool(*x)),
      VmValue::Primitive(x) => match x {
        PrimitiveValue::Bytes(x) => {
          if config.enable_bytes {
            Ok(Self::Bytes(x.clone()))
          } else {
            Ok(Self::String(base64::encode(x)))
          }
        }
        PrimitiveValue::Double(x) => {
          if config.enable_double {
            Ok(Self::Double(f64::from_bits(*x)))
          } else {
            Ok(Self::String(format!("{}", f64::from_bits(*x))))
          }
        }
        PrimitiveValue::Int64(x) => {
          if config.enable_int64 {
            Ok(Self::Int64(*x))
          } else {
            Ok(Self::String(format!("{}", x)))
          }
        }
        PrimitiveValue::String(x) => Ok(Self::String(x.clone())),
      },
      VmValue::List(x) => {
        let out = x
          .node
          .iter()
          .map(|x| Self::encode(&**x, config))
          .collect::<Result<_>>()?;
        Ok(Self::Tagged(TaggedVmValue::L(out)))
      }
      _ => {
        log::debug!("encode: unserializable: {:?}", v);
        Err(SerializeError::Unserializable.into())
      }
    }
  }

  pub fn decode<'a>(&self, ty: &VmType<&'a str>) -> Result<VmValue<'a>> {
    use SerializedVmValue as S;
    match (self, ty) {
      (S::Tagged(TaggedVmValue::M(x)), VmType::Map(map_ty)) => {
        let mut res = VmMapValue {
          elements: Default::default(),
        };
        for (k, field_ty) in map_ty {
          if let Some(v) = x.get(*k) {
            res.elements.insert_mut(*k, Arc::new(v.decode(field_ty)?));
          } else {
            res
              .elements
              .insert_mut(*k, Arc::new(VmValue::Null(field_ty.clone())));
          }
        }
        Ok(VmValue::Map(res))
      }
      (S::Tagged(TaggedVmValue::L(x)), VmType::List(list_ty)) => {
        let res = VmListValue {
          member_ty: (*list_ty.ty).clone(),
          node: x
            .iter()
            .map(|x| x.decode(&*list_ty.ty).map(Arc::new))
            .collect::<Result<_>>()?,
        };
        Ok(VmValue::List(res))
      }
      (S::Null(None), _) => Ok(VmValue::Null(ty.clone())),
      (S::Bool(x), VmType::Bool) => Ok(VmValue::Bool(*x)),
      (S::String(x), VmType::Primitive(PrimitiveType::String)) => {
        Ok(VmValue::Primitive(PrimitiveValue::String(x.clone())))
      }
      (S::Bytes(x), VmType::Primitive(PrimitiveType::String)) => Ok(VmValue::Primitive(
        PrimitiveValue::String(String::from_utf8_lossy(x).to_string()),
      )),
      (S::String(x), VmType::Primitive(PrimitiveType::Int64)) => {
        Ok(VmValue::Primitive(PrimitiveValue::Int64(x.parse()?)))
      }
      (S::Int64(x), VmType::Primitive(PrimitiveType::Int64)) => {
        Ok(VmValue::Primitive(PrimitiveValue::Int64(*x)))
      }
      (S::Double(x), VmType::Primitive(PrimitiveType::Int64)) => {
        Ok(VmValue::Primitive(PrimitiveValue::Int64(*x as i64)))
      }
      (S::String(x), VmType::Primitive(PrimitiveType::Double)) => {
        Ok(VmValue::Primitive(PrimitiveValue::Double(x.parse()?)))
      }
      (S::Int64(x), VmType::Primitive(PrimitiveType::Double)) => Ok(VmValue::Primitive(
        PrimitiveValue::Double((*x as f64).to_bits()),
      )),
      (S::Double(x), VmType::Primitive(PrimitiveType::Double)) => {
        Ok(VmValue::Primitive(PrimitiveValue::Double(x.to_bits())))
      }
      (S::String(x), VmType::Primitive(PrimitiveType::Bytes)) => Ok(VmValue::Primitive(
        PrimitiveValue::Bytes(base64::decode(x)?),
      )),
      (S::Bytes(x), VmType::Primitive(PrimitiveType::Bytes)) => {
        Ok(VmValue::Primitive(PrimitiveValue::Bytes(x.clone())))
      }
      _ => {
        log::debug!("decode: type mismatch: `{:?}`, `{}`", self, ty);
        Err(SerializeError::TypeMismatch.into())
      }
    }
  }
}
