use anyhow::Result;
use rpds::RedBlackTreeMapSync;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, sync::Arc};
use thiserror::Error;

use crate::{
  data::{pathwalker::PathWalker, value::PrimitiveValue},
  schema::compile::{CompiledSchema, FieldType, PrimitiveType},
};

#[derive(Debug)]
pub enum VmValue<'a> {
  Primitive(PrimitiveValue),
  Table(VmTableValue<'a>),
  Set(VmSetValue<'a>),

  /// VM-only
  Bool(bool),

  /// VM-only
  Map(VmMapValue<'a>),

  Null,
}

#[derive(Debug)]
pub struct VmTableValue<'a> {
  pub ty: &'a str,
  pub kind: VmTableValueKind<'a>,
}

#[derive(Debug)]
pub enum VmTableValueKind<'a> {
  Resident(Arc<PathWalker<'a>>),
  Fresh(BTreeMap<&'a str, Arc<VmValue<'a>>>),
}

#[derive(Debug)]
pub struct VmSetValue<'a> {
  pub member_ty: VmType<&'a str>,
  pub kind: VmSetValueKind<'a>,
}

#[derive(Debug)]
pub enum VmSetValueKind<'a> {
  Resident(Arc<PathWalker<'a>>),
  Fresh(Vec<Arc<VmValue<'a>>>),
}

#[derive(Debug)]
pub struct VmMapValue<'a> {
  pub elements: RedBlackTreeMapSync<&'a str, Arc<VmValue<'a>>>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum VmType<K: Clone + Ord + PartialOrd + Eq + PartialEq> {
  Primitive(PrimitiveType),
  Table(VmTableType<K>),
  Set(VmSetType<K>),
  Null,

  /// VM-only
  Bool,

  /// VM-only
  List(Box<VmType<K>>),

  /// VM-only
  Map(RedBlackTreeMapSync<K, VmType<K>>),

  OneOf(Vec<VmType<K>>),
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct VmSetType<K: Clone + Ord + PartialOrd + Eq + PartialEq> {
  pub ty: Box<VmType<K>>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct VmTableType<K> {
  pub name: K,
}

impl<'a, T: AsRef<str> + Clone + Ord + PartialOrd + Eq + PartialEq> From<&'a VmType<T>>
  for VmType<&'a str>
{
  fn from(that: &'a VmType<T>) -> Self {
    match that {
      VmType::Primitive(x) => VmType::Primitive(x.clone()),
      VmType::Table(x) => VmType::Table(VmTableType {
        name: x.name.as_ref(),
      }),
      VmType::Set(x) => VmType::Set(VmSetType {
        ty: Box::new(Self::from(&*x.ty)),
      }),
      VmType::Null => VmType::Null,
      VmType::Bool => VmType::Bool,
      VmType::List(x) => VmType::List(Box::new(Self::from(&**x))),
      VmType::Map(x) => VmType::Map(x.iter().map(|(k, v)| (k.as_ref(), Self::from(v))).collect()),
      VmType::OneOf(x) => VmType::OneOf(x.iter().map(|x| Self::from(x)).collect()),
    }
  }
}

impl<'a> From<&VmValue<'a>> for VmType<&'a str> {
  fn from(that: &VmValue<'a>) -> Self {
    match that {
      VmValue::Primitive(x) => VmType::Primitive(x.get_type()),
      VmValue::Table(x) => VmType::Table(VmTableType { name: x.ty }),
      VmValue::Set(x) => VmType::Set(VmSetType {
        ty: Box::new(x.member_ty.clone()),
      }),
      VmValue::Bool(_) => VmType::Bool,
      VmValue::Map(x) => VmType::Map(
        x.elements
          .iter()
          .map(|(k, v)| (*k, VmType::from(&**v)))
          .collect(),
      ),
      VmValue::Null => VmType::Null,
    }
  }
}

impl<'a, T: From<&'a str> + Clone + Ord + PartialOrd + Eq + PartialEq> From<&'a FieldType>
  for VmType<T>
{
  fn from(that: &'a FieldType) -> Self {
    match that {
      FieldType::Optional(x) => VmType::OneOf(vec![VmType::Null, VmType::from(&**x)]),
      FieldType::Primitive(x) => VmType::Primitive(*x),
      FieldType::Table(x) => VmType::Table(VmTableType {
        name: T::from(&**x),
      }),
      FieldType::Set(x) => VmType::Set(VmSetType {
        ty: Box::new(VmType::from(&**x)),
      }),
    }
  }
}

impl<'a> VmType<&'a str> {
  pub fn is_null(&self) -> bool {
    match self {
      Self::Null => true,
      _ => false,
    }
  }

  pub fn is_covariant_from(&self, that: &VmType<&'a str>) -> bool {
    if self == that {
      true
    } else if let VmType::OneOf(x) = self {
      for elem in x {
        if elem.is_covariant_from(that) {
          return true;
        }
      }
      false
    } else if let VmType::Map(x) = self {
      if let VmType::Map(y) = that {
        for (k_x, v_x) in x {
          if let Some(v_y) = y.get(*k_x) {
            if v_x.is_covariant_from(v_y) {
              continue;
            }
            return false;
          } else {
            return false;
          }
        }
        return true;
      }

      false
    } else {
      false
    }
  }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum VmConst {
  Primitive(PrimitiveValue),
  Table(VmConstTableValue),
  Set(VmConstSetValue),

  Bool(bool),

  Null,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct VmConstTableValue {
  pub ty: String,
  pub fields: BTreeMap<String, VmConst>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct VmConstSetValue {
  pub member_ty: String,
  pub members: Vec<VmConst>,
}

#[derive(Error, Debug)]
pub enum VmValueError {
  #[error("type `{0}` not found in schema")]
  TypeNotFound(String),
  #[error("field `{0}` not found in type `{1}`")]
  FieldNotFound(String, String),
  #[error("field type `{0}` cannot be converted from value type `{1}`")]
  IncompatibleFieldAndValueType(String, String),
  #[error("missing field `{0}` of type `{1}`")]
  MissingField(Arc<str>, Arc<str>),
}

impl<'a> VmValue<'a> {
  pub fn from_const(schema: &'a CompiledSchema, c: &VmConst) -> Result<Self> {
    match c {
      VmConst::Primitive(x) => Ok(Self::Primitive(x.clone())),
      VmConst::Table(x) => {
        let ty = schema
          .types
          .get(x.ty.as_str())
          .ok_or_else(|| VmValueError::TypeNotFound(x.ty.clone()))?;
        let mut fields = BTreeMap::new();
        for (field_name, field_value) in &x.fields {
          let (field_name, (field_expected_ty, _)) =
            ty.fields
              .get_key_value(field_name.as_str())
              .ok_or_else(|| VmValueError::FieldNotFound(field_name.clone(), x.ty.clone()))?;
          let field_value = VmValue::from_const(schema, field_value)?;
          let field_actual_ty = VmType::from(&field_value);
          if !VmType::from(field_expected_ty).is_covariant_from(&field_actual_ty) {
            return Err(
              VmValueError::IncompatibleFieldAndValueType(
                format!("{:?}", field_expected_ty),
                format!("{:?}", field_actual_ty),
              )
              .into(),
            );
          }
          fields.insert(&**field_name, Arc::new(field_value));
        }
        for (name, (field_ty, _)) in &ty.fields {
          if !fields.contains_key(&**name) {
            if let FieldType::Optional(_) = field_ty {
            } else {
              return Err(VmValueError::MissingField(name.clone(), ty.name.clone()).into());
            }
          }
        }
        Ok(Self::Table(VmTableValue {
          ty: &*ty.name,
          kind: VmTableValueKind::Fresh(fields),
        }))
      }
      VmConst::Null => Ok(Self::Null),
      VmConst::Set(x) => {
        let member_ty = schema
          .types
          .get(x.member_ty.as_str())
          .ok_or_else(|| VmValueError::TypeNotFound(x.member_ty.clone()))?;
        let member_ty = VmType::Table(VmTableType {
          name: &*member_ty.name,
        });
        let mut members = Vec::with_capacity(x.members.len());
        for member in &x.members {
          let member = Self::from_const(schema, member)?;
          let member_actual_ty = VmType::from(&member);
          if !member_ty.is_covariant_from(&member_actual_ty) {
            return Err(
              VmValueError::IncompatibleFieldAndValueType(
                format!("{:?}", member_ty),
                format!("{:?}", member_actual_ty),
              )
              .into(),
            );
          }
          members.push(Arc::new(member));
        }
        Ok(Self::Set(VmSetValue {
          member_ty,
          kind: VmSetValueKind::Fresh(members),
        }))
      }
      VmConst::Bool(x) => Ok(Self::Bool(*x)),
    }
  }
}
