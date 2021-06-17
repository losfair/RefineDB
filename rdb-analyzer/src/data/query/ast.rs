pub struct QueryExpr {
  pub segments: Vec<QuerySegment>,
  pub value: Option<Literal>,
}

#[derive(Clone, Eq, PartialEq, Hash)]
pub enum QuerySegment {
  Selector(SelectorExpr),
  Field(String),
}

#[derive(Clone, Eq, PartialEq, Hash)]
pub struct SelectorExpr {
  pub key: String,
  pub condition: SelectorCondition,
  pub value: Literal,
}

#[derive(Clone, Eq, PartialEq, Hash)]
pub enum SelectorCondition {
  Eq,
}

#[derive(Clone, Eq, PartialEq, Hash)]
pub enum Literal {
  Integer(i64),
  String(String),
}
