use lalrpop_util::lalrpop_mod;

pub mod ast;
pub mod planner;

lalrpop_mod!(pub language, "/data/query/language.rs");

use thiserror::Error;

#[derive(Error, Debug)]
pub enum QueryError {
  #[error("invalid literal")]
  InvalidLiteral,
}
