use super::{ast, QueryError};
use crate::{
  data::value::PrimitiveValue,
  schema::compile::{CompiledSchema, FieldAnnotationList, FieldType, PrimitiveType},
  storage_plan::{StorageNode, StoragePlan},
};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use std::{collections::HashMap, convert::TryFrom, sync::Arc};

pub type PointVec = SmallVec<[u8; 36]>;

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
  kind: QueryKind,
  result_ids: Vec<usize>,
  subtree: QuerySubtree,
}

enum QueryKind {
  Get,
  Put(ast::Literal),
}

impl Default for QueryKind {
  fn default() -> Self {
    QueryKind::Get
  }
}

#[derive(Default, Debug, Serialize, Deserialize)]
pub struct QueryPlan {
  pub steps: Vec<QueryStep>,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum PointType {
  Primitive(PrimitiveType),
  Packed(Arc<str>),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum QueryStep {
  /// PointHandle
  ///
  /// Extends the current point with the given slice.
  /// The extended part is removed when the returned `PointHandle` is popped.
  ExtendPoint(PointVec),

  /// PointVec
  CurrentPoint,

  /// PointVec -> Type<PointType>
  PointGet { point_ty: PointType },

  /// PointVec -> Type<PointType> -> ()
  PointPut,

  /// PointVec (start point) -> PointVec (end point) -> ()
  ///
  /// The subplan has the currently scanning point on its stack
  RangeScanIndex { subplan: QueryPlan },

  /// PointVec (start point) -> PointVec (end point) -> ()
  ///
  /// The subplan has the currently scanning point on its stack
  RangeScanKeys { subplan: QueryPlan },

  /// PackedValue -> Type<PointType>
  LensGet {
    path: Vec<Arc<str>>,
    point_ty: PointType,
  },

  /// PackedValue -> Type<PointType> -> ()
  LensPut { path: Vec<Arc<str>> },

  /// typeof(<0>)
  Const(PrimitiveValue),

  /// T -> T
  PeekAndFulfullResult(usize),

  /// T -> U -> (U, T)
  Swap2,

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
      node.kind = QueryKind::Put(x);
    }

    let result_id = self.next_result_id;
    node.result_ids.push(result_id);
    self.next_result_id += 1;
    Ok(result_id)
  }

  pub fn plan(&self) -> Result<QueryPlan> {
    let mut plan = QueryPlan { steps: vec![] };
    for (seg, node) in &self.root.subtree.children {
      let field_name = match seg {
        ast::QuerySegment::Field(x) => x,
        ast::QuerySegment::Selector(x) => {
          return Err(QueryError::SelectorOnRoot(format!("{:?}", x)).into())
        }
      };
      let ty = self
        .schema
        .exports
        .get(field_name.as_str())
        .ok_or_else(|| QueryError::FieldNotFound(field_name.clone(), "<root>".into()))?;
      let storage = self
        .storage
        .nodes
        .get(field_name.as_str())
        .ok_or_else(|| QueryError::Inconsistency)?;
      let mut storage_stack = vec![storage];
      self.do_plan(&mut plan, seg, node, ty, &mut storage_stack)?;
    }
    Ok(plan)
  }

  /// Recursively generate plan on a given query segment.
  ///
  /// All parameters should be consistent.
  fn do_plan(
    &self,
    plan: &mut QueryPlan,
    query_seg: &ast::QuerySegment,
    query_node: &QueryNode,
    ty: &FieldType,
    storage_stack: &mut Vec<&StorageNode>,
  ) -> Result<()> {
    let storage = *storage_stack.last().unwrap();
    if let Some(x) = storage.key {
      plan
        .steps
        .push(QueryStep::ExtendPoint(PointVec::from_slice(&x)));
    }

    match &query_node.kind {
      QueryKind::Get => match ty.optional_unwrapped() {
        FieldType::Primitive(x) => {
          // This is a field of primitive type
          if !query_node.subtree.children.is_empty() {
            return Err(
              QueryError::AttemptSubqueryOnPrimitiveField(
                format!("{:?}", query_seg),
                format!("{}", ty),
              )
              .into(),
            );
          }

          plan.steps.push(QueryStep::CurrentPoint);
          plan.steps.push(QueryStep::PointGet {
            point_ty: PointType::Primitive(x.clone()),
          });
          for &id in &query_node.result_ids {
            plan.steps.push(QueryStep::PeekAndFulfullResult(id));
          }
          plan.steps.push(QueryStep::Pop);
        }
        FieldType::Named(type_name) => {
          // This is a named type - let's get its fields.
          let specialized_ty = self
            .schema
            .types
            .get(type_name)
            .ok_or_else(|| QueryError::Inconsistency)?;

          // Then, iterate over all its child queries.
          for (child_seg, child_node) in &query_node.subtree.children {
            match child_seg {
              ast::QuerySegment::Field(field_name) => {
                // This is a named type. Only the `Field` type of selector can be used.
                // Resolve type and storage plan of the queried field.
                let field_type =
                  specialized_ty
                    .fields
                    .get(field_name.as_str())
                    .ok_or_else(|| {
                      QueryError::FieldNotFound(field_name.clone(), type_name.clone())
                    })?;
                let field_storage = resolve_subspace_reference(
                  storage
                    .children
                    .get(field_name.as_str())
                    .ok_or_else(|| QueryError::Inconsistency)?,
                  storage_stack,
                )?;

                // TODO: Packed
                if field_type.1.as_slice().is_packed() {
                  return Err(QueryError::PackedFieldUnsupported(field_name.clone()).into());
                }

                storage_stack.push(field_storage);

                // Then, recurse into the field.
                self.do_plan(plan, child_seg, child_node, &field_type.0, storage_stack)?;
                storage_stack.pop().unwrap();
              }
              _ => {
                return Err(
                  QueryError::QueryNamedTypeWithNonField(
                    format!("{:?}", query_seg),
                    type_name.clone(),
                    format!("{:?}", child_seg),
                  )
                  .into(),
                );
              }
            }
          }
        }
        FieldType::Set(member_ty) => {
          // This is a set. And the member type is always a named type (test `no_primitive_types_in_set`).
          let member_ty_name = if let FieldType::Named(x) = &**member_ty {
            x
          } else {
            return Err(QueryError::Inconsistency.into());
          };

          let member_specialized_ty = self
            .schema
            .types
            .get(member_ty_name)
            .ok_or_else(|| QueryError::Inconsistency)?;

          let member_storage = resolve_subspace_reference(
            storage
              .set
              .as_ref()
              .ok_or_else(|| QueryError::Inconsistency)?,
            storage_stack,
          )?;

          // Iterate over all its child queries.
          for (child_seg, child_node) in &query_node.subtree.children {
            // Generate a subplan.
            let mut subplan = QueryPlan::default();
            storage_stack.push(member_storage);
            self.do_plan(
              &mut subplan,
              child_seg,
              child_node,
              member_ty,
              storage_stack,
            )?;
            storage_stack.pop().unwrap();

            // Is there any index to use?
            if let ast::QuerySegment::Selector(expr) = child_seg {
              if let Some(index_info) = member_specialized_ty.lookup_indexed_field(&expr.key) {
                // Got the index! Let's use it.
                let index_storage = resolve_subspace_reference(
                  member_storage
                    .children
                    .get(expr.key.as_str())
                    .ok_or_else(|| QueryError::Inconsistency)?,
                  storage_stack,
                )?;

                let index_storage_key =
                  index_storage.key.ok_or_else(|| QueryError::Inconsistency)?;

                let value = PrimitiveValue::try_from((&expr.value, index_info.ty, self.schema))?;

                // The index key format: 0x01 storage_key(12b) value 0x00 index_id(16b)
                // Build the initial index
                let mut index_prefix = PointVec::new();
                index_prefix.extend_from_slice(&[0x01]);
                index_prefix.extend_from_slice(&index_storage_key);
                index_prefix.extend_from_slice(value.serialize_raw().as_slice());
                plan.steps.push(QueryStep::ExtendPoint(index_prefix));

                // Then, the real indices for start/end points...
                plan
                  .steps
                  .push(QueryStep::ExtendPoint(PointVec::from_slice(&[0x00u8])));
                plan.steps.push(QueryStep::CurrentPoint); // start_point
                plan.steps.push(QueryStep::Swap2);
                plan.steps.push(QueryStep::Pop);
                plan.steps.push(QueryStep::Swap2);
                plan
                  .steps
                  .push(QueryStep::ExtendPoint(PointVec::from_slice(&[0x01u8])));
                plan.steps.push(QueryStep::CurrentPoint); // end_point
                plan.steps.push(QueryStep::Swap2);
                plan.steps.push(QueryStep::Pop);
                plan.steps.push(QueryStep::Swap2);

                plan.steps.push(QueryStep::Pop);

                // Now we have (start_point, end_point) on the top of the stack
                // Let's do range scan!
                let step = QueryStep::RangeScanIndex { subplan };
                plan.steps.push(step);

                continue;
              }
            }

            // Do a full set scan.
            todo!()
          }
        }
        _ => {}
      },
      QueryKind::Put(_) => {
        todo!()
      }
    }

    if storage.key.is_some() {
      plan.steps.push(QueryStep::Pop);
    }
    Ok(())
  }
}

fn resolve_subspace_reference<'a>(
  source: &'a StorageNode,
  stack: &Vec<&'a StorageNode>,
) -> Result<&'a StorageNode> {
  if source.subspace_reference {
    let key = source.key.ok_or_else(|| QueryError::Inconsistency)?;
    for x in stack.iter().rev() {
      if x.key == Some(key) {
        if x.subspace_reference {
          return Err(QueryError::Inconsistency.into());
        }
        return Ok(*x);
      }
    }
    return Err(QueryError::Inconsistency.into());
  } else {
    Ok(source)
  }
}
