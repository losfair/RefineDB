use bumpalo::collections::vec::Vec;

pub struct Schema<'a> {
  pub items: Vec<'a, SchemaItem<'a>>,
}

pub enum SchemaItem<'a> {
  Type(&'a TypeItem<'a>),
  Export(&'a ExportItem<'a>),
}

pub struct TypeItem<'a> {
  pub annotations: Vec<'a, Annotation<'a>>,
  pub location: usize,
  pub name: Identifier<'a>,
  pub generics: Vec<'a, Identifier<'a>>,
  pub fields: Vec<'a, TypeField<'a>>,
}

pub struct ExportItem<'a> {
  pub location: usize,
  pub ty: TypeExpr<'a>,
  pub table_name: Identifier<'a>,
}

pub struct TypeField<'a> {
  pub annotations: Vec<'a, Annotation<'a>>,
  pub location: usize,
  pub name: Identifier<'a>,
  pub value: TypeExpr<'a>,
  pub optional: bool,
}

pub enum TypeExpr<'a> {
  Unit(Identifier<'a>),
  Specialize(Identifier<'a>, Vec<'a, TypeExpr<'a>>),
}

pub struct Annotation<'a> {
  pub name: Identifier<'a>,
  pub args: Vec<'a, Literal<'a>>,
}

pub struct Identifier<'a>(pub &'a str);

pub enum Literal<'a> {
  Integer(i64),
  String(&'a str),
  Bytes(&'a [u8]),
}
