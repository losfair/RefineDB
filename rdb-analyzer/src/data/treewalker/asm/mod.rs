use lalrpop_util::lalrpop_mod;

pub mod ast;
mod state;

lalrpop_mod!(pub language, "/data/treewalker/asm/language.rs");

use thiserror::Error;

#[derive(Error, Debug)]
pub enum TwAsmError {
  #[error("invalid literal")]
  InvalidLiteral,
}
