use std::sync::Arc;

use lalrpop_util::lalrpop_mod;

pub mod ast;
pub mod planner;

#[cfg(test)]
mod planner_test;

lalrpop_mod!(pub language, "/data/query/language.rs");

use thiserror::Error;

#[derive(Error, Debug)]
pub enum QueryError {
  #[error("invalid literal")]
  InvalidLiteral,

  #[error("inconsistency detected between schema and storage plan")]
  Inconsistency,

  #[error(
    "segment `{0}` refers to a named type `{1}` and can only be queried by a field. got `{2}`"
  )]
  QueryNamedTypeWithNonField(String, Arc<str>, String),

  #[error("field `{0}` not found on type `{1}`")]
  FieldNotFound(String, Arc<str>),

  #[error("attempting to do subquery on primitive field `{0}` of type `{1}`")]
  AttemptSubqueryOnPrimitiveField(String, String),

  #[error("selectors cannot be used at the root level: `{0}`")]
  SelectorOnRoot(String),

  #[error("packed fields are not yet supported: `{0}`")]
  PackedFieldUnsupported(String),
}
