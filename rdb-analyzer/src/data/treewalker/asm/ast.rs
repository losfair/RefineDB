use bumpalo::collections::vec::Vec;

use crate::schema::compile::PrimitiveType;

pub struct Root<'a> {
  pub graphs: Vec<'a, Graph<'a>>,
}

pub struct Graph<'a> {
  pub name: &'a str,
  pub params: Vec<'a, (&'a str, Option<Type<'a>>)>,
  pub return_type: Option<Type<'a>>,
  pub stmts: Vec<'a, Stmt<'a>>,
}

pub struct Stmt<'a> {
  pub location: usize,
  pub kind: StmtKind<'a>,
}

pub enum StmtKind<'a> {
  Return {
    name: &'a str,
  },
  Node {
    name: Option<&'a str>,
    value: Expr<'a>,
  },
  If {
    precondition: &'a str,
    if_body: Vec<'a, Stmt<'a>>,
    else_body: Option<Vec<'a, Stmt<'a>>>,
  },
}

pub struct Expr<'a> {
  pub location_start: usize,
  pub location_end: usize,
  pub kind: ExprKind<'a>,
}

pub enum Type<'a> {
  Table {
    name: &'a str,
    params: Vec<'a, Type<'a>>,
  },
  Primitive(PrimitiveType),
  Set(&'a Type<'a>),
  Map(Vec<'a, (&'a str, Type<'a>)>),
}

pub enum ExprKind<'a> {
  LoadParam(&'a str),
  LoadConst(Literal<'a>),
  BuildTable(&'a str, &'a str),
  CreateMap,
  GetField(&'a str, &'a str),
  GetSetElement(&'a str, &'a str),
  InsertIntoMap(&'a str, &'a str, &'a str),
  InsertIntoTable(&'a str, &'a str, &'a str),
  InsertIntoSet(&'a str, &'a str),
  DeleteFromSet(&'a str, &'a str),
  DeleteFromMap(&'a str, &'a str),
  DeleteFromTable(&'a str, &'a str),
  Eq(&'a str, &'a str),
  Ne(&'a str, &'a str),
  And(&'a str, &'a str),
  Or(&'a str, &'a str),
  Not(&'a str),
  UnwrapOptional(&'a str),
  Select(&'a str, &'a str),
}

pub enum Literal<'a> {
  Null,
  Bool(bool),
  Integer(i64),
  HexBytes(&'a [u8]),
  String(&'a str),
}
