pub struct Schema {
  pub items: Vec<SchemaItem>,
}

pub enum SchemaItem {
  Type(TypeItem),
}

pub struct TypeItem {
  pub annotations: Vec<Annotation>,
  pub location: usize,
  pub name: Identifier,
  pub generics: Vec<Identifier>,
}

pub struct TypeField {
  pub annotations: Vec<Annotation>,
  pub location: usize,
  pub name: Identifier,
  pub value: TypeExpr,
}

pub enum TypeExpr {
  Unit(Identifier),
  Specialize(Identifier, Vec<TypeExpr>),
}

pub struct Annotation {
  pub name: Identifier,
  pub args: Vec<Literal>,
}

pub struct Identifier(pub String);

pub enum Literal {
  Integer(i64),
  String(String),
}
