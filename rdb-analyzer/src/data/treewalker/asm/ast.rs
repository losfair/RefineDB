use bumpalo::collections::vec::Vec;

use crate::schema::compile::PrimitiveType;

pub struct Root<'a> {
  pub graphs: Vec<'a, &'a Graph<'a>>,
  pub type_aliases: Vec<'a, &'a TypeAlias<'a>>,
}

pub struct TypeAlias<'a> {
  pub name: &'a str,
  pub ty: Type<'a>,
}

pub enum Item<'a> {
  Graph(&'a Graph<'a>),
  TypeAlias(&'a TypeAlias<'a>),
}

pub struct Graph<'a> {
  pub name: &'a str,
  pub exported: bool,
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
    value: Expr<'a>,
  },
  Node {
    name: Option<&'a str>,
    value: Expr<'a>,
  },
  If {
    precondition: Expr<'a>,
    if_body: Vec<'a, Stmt<'a>>,
    else_body: Option<Vec<'a, Stmt<'a>>>,
  },
  Throw {
    value: Expr<'a>,
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
  List(&'a Type<'a>),
  Map(Vec<'a, (&'a str, Type<'a>)>),
  Bool,
  Schema,
}

pub enum ExprKind<'a> {
  LoadConst(Literal<'a>),
  BuildTable(Type<'a>, &'a Expr<'a>),
  BuildSet(&'a Expr<'a>),
  CreateMap,
  GetField(&'a str, &'a Expr<'a>),
  GetSetElement(&'a Expr<'a>, &'a Expr<'a>),
  InsertIntoMap(&'a str, &'a Expr<'a>, &'a Expr<'a>),
  InsertIntoTable(&'a str, &'a Expr<'a>, &'a Expr<'a>),
  InsertIntoSet(&'a Expr<'a>, &'a Expr<'a>),
  DeleteFromSet(&'a Expr<'a>, &'a Expr<'a>),
  DeleteFromMap(&'a str, &'a Expr<'a>),
  DeleteFromTable(&'a str, &'a Expr<'a>),
  Eq(&'a Expr<'a>, &'a Expr<'a>),
  Ne(&'a Expr<'a>, &'a Expr<'a>),
  And(&'a Expr<'a>, &'a Expr<'a>),
  Or(&'a Expr<'a>, &'a Expr<'a>),
  Not(&'a Expr<'a>),
  Select(&'a Expr<'a>, &'a Expr<'a>),
  Node(&'a str),
  IsPresent(&'a Expr<'a>),
  IsNull(&'a Expr<'a>),
  OrElse(&'a Expr<'a>, &'a Expr<'a>),
  Call(&'a str, Vec<'a, Expr<'a>>),
  Add(&'a Expr<'a>, &'a Expr<'a>),
  Sub(&'a Expr<'a>, &'a Expr<'a>),
  CreateList(Type<'a>),
  Reduce(&'a str, &'a Expr<'a>, &'a Expr<'a>, &'a Expr<'a>),
  RangeReduce(
    &'a str,
    &'a Expr<'a>,
    &'a Expr<'a>,
    &'a Expr<'a>,
    &'a Expr<'a>,
    &'a Expr<'a>,
  ),
  Prepend(&'a Expr<'a>, &'a Expr<'a>),
  Pop(&'a Expr<'a>),
  Head(&'a Expr<'a>),
}

pub enum Literal<'a> {
  Null(Type<'a>),
  Bool(bool),
  Integer(i64),
  HexBytes(&'a [u8]),
  String(&'a str),
  EmptySet(Type<'a>),
}
