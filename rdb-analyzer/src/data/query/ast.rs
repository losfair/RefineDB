use crate::{
  data::value::PrimitiveValue,
  schema::compile::{CompiledSchema, FieldType, PrimitiveType},
};
use std::convert::TryFrom;
use thiserror::Error;

pub struct QueryExpr {
  pub segments: Vec<QuerySegment>,
  pub value: Option<Literal>,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum QuerySegment {
  Selector(SelectorExpr),
  Field(String),
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct SelectorExpr {
  pub key: String,
  pub condition: SelectorCondition,
  pub value: Literal,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum SelectorCondition {
  Eq,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum Literal {
  Integer(i64),
  String(String),
}

#[derive(Debug, Error)]
pub enum LiteralParseError {
  #[error("cannot parse literal `{0}` as type `{1}`")]
  TypeMismatch(String, String),
}

impl TryFrom<(&Literal, &FieldType, &CompiledSchema)> for PrimitiveValue {
  type Error = LiteralParseError;

  fn try_from(
    (value, ty, _): (&Literal, &FieldType, &CompiledSchema),
  ) -> Result<Self, Self::Error> {
    Ok(match (value, ty) {
      (Literal::Integer(x), FieldType::Primitive(PrimitiveType::Int64)) => Self::Int64(*x),
      (Literal::String(x), FieldType::Primitive(PrimitiveType::String)) => Self::String(x.clone()),
      (Literal::String(x), FieldType::Primitive(PrimitiveType::Bytes)) => {
        Self::Bytes(Vec::from(x.clone()))
      }
      _ => {
        return Err(LiteralParseError::TypeMismatch(
          format!("{:?}", value),
          format!("{:?}", ty),
        ))
      }
    })
  }
}
