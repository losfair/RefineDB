use anyhow::Result;
use rpds::RedBlackTreeMapSync;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, sync::Arc};
use thiserror::Error;

use crate::{
  data::value::PrimitiveValue,
  schema::compile::{CompiledSchema, FieldType, PrimitiveType},
};

#[derive(Debug)]
pub enum VmValue<'a> {
  Primitive(PrimitiveValue),
  Table(VmTableValue<'a>),
  Set(VmSetValue<'a>),

  Map(VmMapValue<'a>),

  Null,
}

#[derive(Debug)]
pub struct VmResidentPath<'a> {
  pub storage_key: &'a [u8],
  pub prev: Option<Arc<VmResidentPath<'a>>>,
}

#[derive(Debug)]
pub struct VmTableValue<'a> {
  pub ty: &'a str,
  pub kind: VmTableValueKind<'a>,
}

#[derive(Debug)]
pub enum VmTableValueKind<'a> {
  Resident(VmResidentPath<'a>),
  Fresh(BTreeMap<&'a str, VmValue<'a>>),
}

#[derive(Debug)]
pub struct VmSetValue<'a> {
  pub member_ty: VmType<'a>,
  pub kind: VmSetValueKind<'a>,
}

#[derive(Debug)]
pub enum VmSetValueKind<'a> {
  Resident(VmResidentPath<'a>),
  Fresh(Vec<VmValue<'a>>),
}

#[derive(Debug)]
pub struct VmMapValue<'a> {
  pub elements: RedBlackTreeMapSync<&'a str, VmValue<'a>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum VmType<'a> {
  Primitive(PrimitiveType),
  Table(&'a str),
  Set(Box<VmType<'a>>),
  Null,

  /// VM-only
  List(Box<VmType<'a>>),

  /// VM-only
  Map(RedBlackTreeMapSync<&'a str, VmType<'a>>),

  OneOf(Vec<VmType<'a>>),
}

impl<'a> From<&VmValue<'a>> for VmType<'a> {
  fn from(that: &VmValue<'a>) -> Self {
    match that {
      VmValue::Primitive(x) => VmType::Primitive(x.get_type()),
      VmValue::Table(x) => VmType::Table(x.ty),
      VmValue::Set(x) => VmType::Set(Box::new(x.member_ty.clone())),
      VmValue::Map(x) => VmType::Map(
        x.elements
          .iter()
          .map(|(k, v)| (*k, VmType::from(v)))
          .collect(),
      ),
      VmValue::Null => VmType::Null,
    }
  }
}

impl<'a> From<&'a FieldType> for VmType<'a> {
  fn from(that: &'a FieldType) -> Self {
    match that {
      FieldType::Optional(x) => VmType::OneOf(vec![VmType::Null, VmType::from(&**x)]),
      FieldType::Primitive(x) => VmType::Primitive(*x),
      FieldType::Table(x) => VmType::Table(&**x),
      FieldType::Set(x) => VmType::Set(Box::new(VmType::from(&**x))),
    }
  }
}

impl<'a> VmType<'a> {
  pub fn is_covariant_from(&self, that: &VmType<'a>) -> bool {
    if self == that {
      true
    } else if let VmType::OneOf(x) = self {
      for elem in x {
        if elem.is_covariant_from(that) {
          return true;
        }
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
          fields.insert(&**field_name, field_value);
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
        let member_ty = VmType::Table(&*member_ty.name);
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
          members.push(member);
        }
        Ok(Self::Set(VmSetValue {
          member_ty,
          kind: VmSetValueKind::Fresh(members),
        }))
      }
    }
  }
}
