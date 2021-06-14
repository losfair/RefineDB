use thiserror::Error;

#[derive(Error, Debug)]
pub enum SchemaError {
  #[error("invalid literal")]
  InvalidLiteral,
}
