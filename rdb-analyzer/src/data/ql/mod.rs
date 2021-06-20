use lalrpop_util::lalrpop_mod;

pub mod ast;
mod state;

lalrpop_mod!(pub language, "/data/ql/language.rs");

use thiserror::Error;

#[derive(Error, Debug)]
pub enum QlError {
  #[error("invalid literal")]
  InvalidLiteral,
}
