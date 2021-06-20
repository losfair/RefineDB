use bumpalo::collections::vec::Vec;

pub struct QlRoot<'a> {
  pub graphs: Vec<'a, QlGraph<'a>>,
}

pub struct QlGraph<'a> {
  pub name: &'a str,
}

pub enum Literal<'a> {
  Integer(i64),
  HexBytes(&'a [u8]),
  String(&'a str),
}
