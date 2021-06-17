use anyhow::Result;

use super::ast;
use crate::{
  schema::compile::{CompiledSchema, FieldType},
  storage_plan::{StorageNode, StoragePlan},
};
use std::{collections::HashMap, sync::Arc};

pub struct QueryPlanner<'a> {
  schema: &'a CompiledSchema,
  storage: &'a StoragePlan,

  root: QueryNode,
  next_result_id: usize,
}

#[derive(Default)]
struct QuerySubtree {
  children: HashMap<ast::QuerySegment, QueryNode>,
}

#[derive(Default)]
struct QueryNode {
  value: Option<ast::Literal>,
  result_ids: Vec<usize>,
  subtree: QuerySubtree,
}

pub struct QueryPlan {
  pub steps: Vec<QueryStep>,
}

pub enum QueryStep {
  /// Vec<u8>
  PointGet { point_key: Vec<u8> },

  /// Vec<u8> -> ()
  PointSet { point_key: Vec<u8> },

  /// Vec<u8> -> PackedValue
  UnpackAs { type_name: Arc<str> },

  /// PackedValue -> Vec<u8>
  PackAs { type_name: Arc<str> },

  /// PackedValue -> Vec<u8>
  LensGet { path: Vec<Arc<str>> },

  /// PackedValue -> Vec<u8> -> ()
  LensSet { path: Vec<Arc<str>> },

  /// Vec<u8>
  Const(ast::Literal),

  /// Vec<u8> -> ()
  FulfullResult(usize),

  /// any -> ()
  Pop,
}

impl<'a> QueryPlanner<'a> {
  pub fn new(schema: &'a CompiledSchema, storage: &'a StoragePlan) -> Self {
    Self {
      schema,
      storage,
      root: QueryNode::default(),
      next_result_id: 1,
    }
  }

  pub fn add_query(&mut self, query: &str) -> Result<usize> {
    let query = super::language::QueryExprParser::new()
      .parse(query)
      .map_err(|x| x.map_token(|x| x.to_string()))?;

    let mut node = &mut self.root;
    for seg in query.segments {
      node = node
        .subtree
        .children
        .entry(seg)
        .or_insert(QueryNode::default());
    }

    if let Some(x) = query.value {
      node.value = Some(x);
    }

    let result_id = self.next_result_id;
    node.result_ids.push(result_id);
    self.next_result_id += 1;
    Ok(result_id)
  }

  pub fn plan(&self) -> Result<QueryPlan> {
    let mut plan = QueryPlan { steps: vec![] };
    todo!()
  }

  fn do_plan(
    &self,
    plan: &mut QueryPlan,
    query_seg: &ast::QuerySegment,
    query_node: &QueryNode,
    ty: &FieldType,
    storage: &StorageNode,
  ) -> Result<()> {
    Ok(())
  }
}
