use anyhow::Result;
use rpds::RedBlackTreeMapSync;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, fmt::Display, sync::Arc};
use thiserror::Error;

use crate::{
  data::{pathwalker::PathWalker, value::PrimitiveValue},
  schema::compile::{CompiledSchema, FieldAnnotationList, FieldType, PrimitiveType},
};

#[derive(Debug, PartialEq)]
pub enum VmValue<'a> {
  Primitive(PrimitiveValue),
  Table(VmTableValue<'a>),
  Set(VmSetValue<'a>),

  /// VM-only
  Bool(bool),

  /// VM-only
  Map(VmMapValue<'a>),

  Null(VmType<&'a str>),

  List(VmListValue<'a>),
}

#[derive(Debug, PartialEq)]
pub struct VmListValue<'a> {
  pub member_ty: VmType<&'a str>,
  pub node: Option<Arc<VmListNode<'a>>>,
}

#[derive(Debug, PartialEq)]
pub struct VmListNode<'a> {
  pub value: Arc<VmValue<'a>>,
  pub next: Option<Arc<VmListNode<'a>>>,
}

#[derive(Debug, PartialEq)]
pub struct VmTableValue<'a> {
  pub ty: &'a str,
  pub kind: VmTableValueKind<'a>,
}

#[derive(Debug, PartialEq)]
pub enum VmTableValueKind<'a> {
  Resident(Arc<PathWalker<'a>>),
  Fresh(BTreeMap<&'a str, Arc<VmValue<'a>>>),
}

#[derive(Debug, PartialEq)]
pub struct VmSetValue<'a> {
  pub member_ty: VmType<&'a str>,
  pub kind: VmSetValueKind<'a>,
}

#[derive(Debug, PartialEq)]
pub enum VmSetValueKind<'a> {
  Resident(Arc<PathWalker<'a>>),
  Fresh(BTreeMap<Vec<u8>, Arc<VmValue<'a>>>),
}

#[derive(Debug, PartialEq)]
pub struct VmMapValue<'a> {
  pub elements: RedBlackTreeMapSync<&'a str, Arc<VmValue<'a>>>,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Hash)]
pub enum VmType<K: Clone + Ord + PartialOrd + Eq + PartialEq> {
  Primitive(PrimitiveType),
  Table(VmTableType<K>),
  Set(VmSetType<K>),

  /// VM-only
  Bool,

  /// VM-only
  List(VmListType<K>),

  /// VM-only
  Map(RedBlackTreeMapSync<K, VmType<K>>),

  /// An unknown type. Placeholder for unfinished type inference.
  Unknown,

  /// The schema type. Placeholder.
  Schema,
}

impl<K: AsRef<str> + Clone + Ord + PartialOrd + Eq + PartialEq> Display for VmType<K> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      VmType::Primitive(x) => write!(f, "{}", x),
      VmType::Table(x) => write!(f, "{}", x.name.as_ref()),
      VmType::Unknown => write!(f, "unknown"),
      VmType::Bool => write!(f, "bool"),
      VmType::Map(m) => {
        write!(f, "map {{")?;
        for (k, v) in m {
          write!(f, " {}: {},", k.as_ref(), v)?;
        }
        write!(f, " }}")?;
        Ok(())
      }
      VmType::List(x) => write!(f, "list<{}>", x.ty),
      VmType::Set(x) => write!(f, "set<{}>", x.ty),
      VmType::Schema => write!(f, "schema"),
    }
  }
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Hash)]
pub struct VmSetType<K: Clone + Ord + PartialOrd + Eq + PartialEq> {
  pub ty: Box<VmType<K>>,
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Hash)]
pub struct VmListType<K: Clone + Ord + PartialOrd + Eq + PartialEq> {
  pub ty: Box<VmType<K>>,
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Hash)]
pub struct VmTableType<K> {
  pub name: K,
}

impl<
    'a,
    T: AsRef<str> + Clone + Ord + PartialOrd + Eq + PartialEq,
    U: From<&'a str> + Clone + Ord + PartialOrd + Eq + PartialEq,
  > From<&'a VmType<T>> for VmType<U>
{
  fn from(that: &'a VmType<T>) -> Self {
    match that {
      VmType::Primitive(x) => VmType::Primitive(x.clone()),
      VmType::Table(x) => VmType::Table(VmTableType {
        name: U::from(x.name.as_ref()),
      }),
      VmType::Set(x) => VmType::Set(VmSetType {
        ty: Box::new(Self::from(&*x.ty)),
      }),
      VmType::Bool => VmType::Bool,
      VmType::List(x) => VmType::List(VmListType {
        ty: Box::new(Self::from(&*x.ty)),
      }),
      VmType::Map(x) => VmType::Map(
        x.iter()
          .map(|(k, v)| (U::from(k.as_ref()), Self::from(v)))
          .collect(),
      ),
      VmType::Unknown => VmType::Unknown,
      VmType::Schema => VmType::Schema,
    }
  }
}

impl<'a, T: From<&'a str> + Clone + Ord + PartialOrd + Eq + PartialEq> From<&'a CompiledSchema>
  for VmType<T>
{
  fn from(that: &'a CompiledSchema) -> Self {
    let mut m = RedBlackTreeMapSync::new_sync();
    for (field_name, field_ty) in &that.exports {
      m.insert_mut(T::from(&**field_name), VmType::<T>::from(field_ty));
    }
    VmType::Map(m)
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
      VmValue::Null(x) => x.clone(),
      VmValue::List(x) => x.member_ty.clone(),
    }
  }
}

impl<'a, T: From<&'a str> + Clone + Ord + PartialOrd + Eq + PartialEq> From<&'a FieldType>
  for VmType<T>
{
  fn from(that: &'a FieldType) -> Self {
    match that {
      FieldType::Optional(x) => VmType::from(&**x),
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
  pub fn is_covariant_from(&self, that: &VmType<&'a str>) -> bool {
    if self == that {
      true
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

  pub fn set_primary_key(&self, schema: &'a CompiledSchema) -> Option<(&'a str, &'a FieldType)> {
    match self {
      VmType::Set(x) => match &*x.ty {
        VmType::Table(x) => {
          let specialized_ty = schema.types.get(x.name)?;
          specialized_ty
            .fields
            .iter()
            .find_map(|(name, (ty, ann))| ann.as_slice().is_primary().then(|| (&**name, ty)))
        }
        _ => None,
      },
      _ => None,
    }
  }

  pub fn default_value(&self) -> Option<Arc<VmValue<'a>>> {
    Some(Arc::new(match self {
      VmType::Bool => VmValue::Bool(false),
      VmType::List(_) => return None,
      VmType::Map(x) => VmValue::Map(VmMapValue {
        elements: x
          .iter()
          .map(|(k, v)| v.default_value().map(|v| (*k, v)))
          .collect::<Option<_>>()?,
      }),
      VmType::Primitive(x) => VmValue::Primitive(PrimitiveValue::default_value_for_type(*x)),
      VmType::Schema => return None,
      VmType::Set(ty) => VmValue::Set(VmSetValue {
        member_ty: (*ty.ty).clone(),
        kind: VmSetValueKind::Fresh(BTreeMap::new()),
      }),
      VmType::Table(x) => VmValue::Table(VmTableValue {
        ty: x.name,
        kind: VmTableValueKind::Fresh(BTreeMap::new()),
      }),
      VmType::Unknown => return None,
    }))
  }
}

#[derive(Serialize, Deserialize, Clone, Eq, PartialEq, Hash, Debug)]
pub enum VmConst {
  Primitive(PrimitiveValue),
  Table(VmConstTableValue),
  Set(VmConstSetValue),

  Bool(bool),

  Null(VmType<String>),
}

#[derive(Serialize, Deserialize, Clone, Eq, PartialEq, Hash, Debug)]
pub struct VmConstTableValue {
  pub ty: String,
  pub fields: BTreeMap<String, VmConst>,
}

#[derive(Serialize, Deserialize, Clone, Eq, PartialEq, Hash, Debug)]
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
  #[error("primary key not found in a set member type")]
  MissingPrimaryKey,
}

impl<'a> VmValue<'a> {
  pub fn is_null(&self) -> bool {
    match self {
      VmValue::Null(_) => true,
      _ => false,
    }
  }

  pub fn from_const(schema: &'a CompiledSchema, c: &'a VmConst) -> Result<Self> {
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
      VmConst::Set(x) => {
        let member_ty = schema
          .types
          .get(x.member_ty.as_str())
          .ok_or_else(|| VmValueError::TypeNotFound(x.member_ty.clone()))?;
        let member_ty = VmType::Table(VmTableType {
          name: &*member_ty.name,
        });
        let (primary_key, _) = VmType::Set(VmSetType {
          ty: Box::new(member_ty.clone()),
        })
        .set_primary_key(schema)
        .ok_or_else(|| VmValueError::MissingPrimaryKey)?;
        let mut members = BTreeMap::new();
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

          // XXX: We checked covariance above but is it enough?
          let primary_key_value = match &member.unwrap_table().kind {
            VmTableValueKind::Fresh(x) => x
              .get(primary_key)
              .unwrap()
              .unwrap_primitive()
              .serialize_for_key_component(),
            _ => unreachable!(),
          };
          members.insert(primary_key_value.to_vec(), Arc::new(member));
        }
        Ok(Self::Set(VmSetValue {
          member_ty,
          kind: VmSetValueKind::Fresh(members),
        }))
      }
      VmConst::Null(x) => Ok(Self::Null(VmType::from(x))),
      VmConst::Bool(x) => Ok(Self::Bool(*x)),
    }
  }

  pub fn unwrap_table<'b>(&'b self) -> &'b VmTableValue<'a> {
    match self {
      VmValue::Table(x) => x,
      _ => panic!("unwrap_table: got non-table type {:?}", self),
    }
  }

  pub fn unwrap_map<'b>(&'b self) -> &'b VmMapValue<'a> {
    match self {
      VmValue::Map(x) => x,
      _ => panic!("unwrap_map: got non-map type {:?}", self),
    }
  }

  pub fn unwrap_set<'b>(&'b self) -> &'b VmSetValue<'a> {
    match self {
      VmValue::Set(x) => x,
      _ => panic!("unwrap_set: got non-set type {:?}", self),
    }
  }

  pub fn unwrap_primitive<'b>(&'b self) -> &'b PrimitiveValue {
    match self {
      VmValue::Primitive(x) => x,
      _ => panic!("unwrap_primitive: got non-primitive type {:?}", self),
    }
  }

  pub fn unwrap_bool<'b>(&'b self) -> bool {
    match self {
      VmValue::Bool(x) => *x,
      _ => panic!("unwrap_bool: got non-bool type {:?}", self),
    }
  }
}
