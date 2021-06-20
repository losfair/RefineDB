use bumpalo::collections::vec::Vec;

pub struct QlRoot<'a> {
  pub graphs: Vec<'a, QlGraph<'a>>,
}

pub struct QlGraph<'a> {
  pub name: &'a str,
  pub params: Vec<'a, (&'a str, QlType<'a>)>,
  pub return_type: Option<QlType<'a>>,
  pub stmts: Vec<'a, QlStmt<'a>>,
}

pub struct QlStmt<'a> {
  pub location: usize,
  pub kind: QlStmtKind<'a>,
}

pub enum QlStmtKind<'a> {
  Let { name: &'a str, value: QlExpr<'a> },
  Assign { base: QlExpr<'a>, value: QlExpr<'a> },
  EffectAssign { base: QlExpr<'a>, value: QlExpr<'a> },
}

pub struct QlExpr<'a> {
  pub location_start: usize,
  pub location_end: usize,
  pub kind: QlExprKind<'a>,
}

pub struct QlType<'a> {
  pub name: &'a str,
  pub params: Vec<'a, QlType<'a>>,
}

pub enum QlExprKind<'a> {
  Primitive(Literal<'a>),
  ValueRef(&'a str),
  GetField(&'a QlExpr<'a>, &'a str),
  NewMap(Vec<'a, (&'a str, QlExpr<'a>)>),
  UnwrapOptional(&'a QlExpr<'a>),
  BuildTable(QlType<'a>, &'a QlExpr<'a>),
  GetSetElement(&'a QlExpr<'a>, &'a str, &'a QlExpr<'a>),
}

pub enum Literal<'a> {
  Integer(i64),
  HexBytes(&'a [u8]),
  String(&'a str),
}
