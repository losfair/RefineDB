use std::{collections::BTreeMap, sync::Arc};

use anyhow::Result;

use crate::{
  data::{treewalker::vm_value::VmMapValue, value::PrimitiveValue},
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
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
pub enum SerializedVmValue {
  Map(BTreeMap<String, SerializedVmValue>),
  String(String),
  Bool(bool),
  Null(Option<Never>),
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Never {}

impl SerializedVmValue {
  pub fn encode(v: &VmValue) -> Result<Self> {
    match v {
      VmValue::Map(x) => Ok(Self::Map(
        x.elements
          .iter()
          .map(|(k, v)| Self::encode(&**v).map(|x| (k.to_string(), x)))
          .collect::<Result<_>>()?,
      )),
      VmValue::Null(_) => Ok(Self::Null(None)),
      VmValue::Bool(x) => Ok(Self::Bool(*x)),
      VmValue::Primitive(x) => match x {
        PrimitiveValue::Bytes(x) => Ok(Self::String(base64::encode(x))),
        PrimitiveValue::Double(x) => Ok(Self::String(format!("{}", f64::from_bits(*x)))),
        PrimitiveValue::Int64(x) => Ok(Self::String(format!("{}", x))),
        PrimitiveValue::String(x) => Ok(Self::String(x.clone())),
      },
      _ => {
        log::debug!("encode: unserializable: {:?}", v);
        Err(SerializeError::Unserializable.into())
      }
    }
  }

  pub fn decode<'a>(&self, ty: &VmType<&'a str>) -> Result<VmValue<'a>> {
    use SerializedVmValue as S;
    match (self, ty) {
      (S::Map(x), VmType::Map(map_ty)) => {
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
      (S::Null(None), _) => Ok(VmValue::Null(ty.clone())),
      (S::Bool(x), VmType::Bool) => Ok(VmValue::Bool(*x)),
      (S::String(x), VmType::Primitive(PrimitiveType::String)) => {
        Ok(VmValue::Primitive(PrimitiveValue::String(x.clone())))
      }
      (S::String(x), VmType::Primitive(PrimitiveType::Int64)) => {
        Ok(VmValue::Primitive(PrimitiveValue::Int64(x.parse()?)))
      }
      (S::String(x), VmType::Primitive(PrimitiveType::Double)) => {
        Ok(VmValue::Primitive(PrimitiveValue::Double(x.parse()?)))
      }
      (S::String(x), VmType::Primitive(PrimitiveType::Bytes)) => Ok(VmValue::Primitive(
        PrimitiveValue::Bytes(base64::decode(x)?),
      )),
      _ => {
        log::debug!("decode: type mismatch: `{:?}`, `{}`", self, ty);
        Err(SerializeError::TypeMismatch.into())
      }
    }
  }
}
