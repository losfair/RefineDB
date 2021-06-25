use lalrpop_util::lalrpop_mod;

mod ast;
pub mod codegen;
mod state;

#[cfg(test)]
mod asm_test;

lalrpop_mod!(language, "/data/treewalker/asm/language.rs");

use thiserror::Error;

#[derive(Error, Debug)]
pub enum TwAsmError {
  #[error("invalid literal")]
  InvalidLiteral,

  #[error("type unsupported in table")]
  TypeUnsupportedInTable,

  #[error("node not found: {0}")]
  NodeNotFound(String),

  #[error("identifier not found: {0}")]
  IdentifierNotFound(String),

  #[error("duplicate return")]
  DuplicateReturn,

  #[error("param not found: {0}")]
  ParamNotFound(String),

  #[error("duplicate param: {0}")]
  DuplicateParam(String),

  #[error("duplicate graph: {0}")]
  DuplicateGraph(String),

  #[error("duplicate node name: {0}")]
  DuplicateNodeName(String),

  #[error("duplicate type alias: {0}")]
  DuplicateTypeAlias(String),

  #[error("graph not found: {0}")]
  GraphNotFound(String),
}
