use std::collections::BTreeMap;
use std::collections::HashMap;
use std::fmt::Display;
use std::sync::Arc;

use anyhow::Result;
use thiserror::Error;

use super::grammar::ast::{self, TypeExpr};
use crate::schema::grammar::ast::Literal;
use crate::schema::grammar::ast::SchemaItem;
use serde::{Deserialize, Serialize};

#[derive(Error, Debug)]
pub enum SchemaCompileError {
  #[error("duplicate type `{0}`")]
  DuplicateType(String),

  #[error("duplicate export `{0}`")]
  DuplicateExport(String),

  #[error("duplicate field `{field}` in type `{ty}`")]
  DuplicateField { field: String, ty: String },

  #[error("recursive types")]
  RecursiveTypes,

  #[error("missing type: {0}")]
  MissingType(String),

  #[error("expecting {expected_args} arguments on type {ty}, got {got_args}")]
  ArgCountMismatch {
    expected_args: usize,
    ty: String,
    got_args: usize,
  },

  #[error("cannot specialize a type parameter `{0}`.")]
  CannotSpecializeTypeParameter(String),

  #[error("cannot specialize a primitive type `{0}`.")]
  CannotSpecializePrimitiveType(String),

  #[error("sets must have exactly one type parameter")]
  BadSetTypeParameter,

  #[error("unknown annotation on field `{0}` of type `{1}`: `{2}`")]
  UnknownAnnotationOnField(String, String, String),

  #[error("field `{0}` of type `{1}`: indexes are only allowed on primitive or packed fields")]
  IndexOnNonPrimitiveOrPackedField(String, String),
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum PrimitiveType {
  Int64,
  Double,
  String,
  Bytes,
}

impl Display for PrimitiveType {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(
      f,
      "{}",
      match self {
        Self::Int64 => "int64",
        Self::Double => "double",
        Self::String => "string",
        Self::Bytes => "bytes",
      }
    )
  }
}

static PRIMITIVE_TYPES: phf::Map<&'static str, PrimitiveType> = phf::phf_map! {
  "int64" => PrimitiveType::Int64,
  "double" => PrimitiveType::Double,
  "string" => PrimitiveType::String,
  "bytes" => PrimitiveType::Bytes,
};

#[derive(Default, Serialize, Deserialize)]
pub struct CompiledSchema {
  pub types: BTreeMap<Arc<str>, SpecializedType>,
  pub exports: BTreeMap<Arc<str>, FieldType>,
}

impl Display for CompiledSchema {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    for (_, ty) in &self.types {
      write!(f, "{}\n", ty)?;
    }
    for (k, v) in &self.exports {
      write!(f, "export {} {};\n", v, k)?;
    }
    Ok(())
  }
}

pub fn compile<'a>(input: &ast::Schema<'a>) -> Result<CompiledSchema> {
  let mut resolution_ctx = TypeResolutionContext::new(input)?;
  let mut result = CompiledSchema {
    types: BTreeMap::new(),
    exports: BTreeMap::new(),
  };

  for item in &input.items {
    match item {
      SchemaItem::Export(x) => {
        if result.exports.contains_key(x.table_name.0) {
          return Err(SchemaCompileError::DuplicateExport(x.table_name.0.to_string()).into());
        }
        let ty = resolution_ctx.resolve_type_expr(&HashMap::new(), &x.ty)?;
        result.exports.insert(Arc::from(x.table_name.0), ty);
      }
      _ => {}
    }
  }
  result.types = resolution_ctx.resolved.clone();
  Ok(result)
}

#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct SpecializedType {
  pub name: Arc<str>,
  pub fields: BTreeMap<Arc<str>, (FieldType, Vec<FieldAnnotation>)>,
}

#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum FieldAnnotation {
  Unique,
  Index,
  Packed,
  RenameFrom(String),
}

impl FieldAnnotation {
  pub fn is_packed(&self) -> bool {
    match self {
      FieldAnnotation::Packed => true,
      _ => false,
    }
  }
  pub fn is_index(&self) -> bool {
    match self {
      FieldAnnotation::Index => true,
      _ => false,
    }
  }
  pub fn is_unique(&self) -> bool {
    match self {
      FieldAnnotation::Unique => true,
      _ => false,
    }
  }
}

impl Display for FieldAnnotation {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      Self::Unique => write!(f, "@unique"),
      Self::Index => write!(f, "@index"),
      Self::Packed => write!(f, "@packed"),
      Self::RenameFrom(x) => write!(f, "@rename_from({})", serde_json::to_string(x).unwrap()),
    }
  }
}

#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum FieldType {
  Named(Arc<str>),
  Primitive(PrimitiveType),
  Set(Box<FieldType>),
  Optional(Box<FieldType>),
}

impl FieldType {
  pub fn optional_unwrapped(&self) -> &Self {
    match self {
      Self::Optional(x) => &**x,
      _ => self,
    }
  }
}

impl Display for FieldType {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      Self::Named(x) => write!(f, "{}", x),
      Self::Primitive(x) => write!(f, "{}", x),
      Self::Set(x) => write!(f, "set<{}>", x),
      Self::Optional(x) => write!(f, "{}?", x),
    }
  }
}

impl Display for SpecializedType {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "type {} {{\n", self.name)?;
    for (k, (ty, annotations)) in &self.fields {
      write!(f, "  ")?;
      for x in annotations {
        write!(f, "{} ", x)?;
      }
      write!(f, "{}: {},\n", k, ty)?;
    }
    write!(f, "}}\n")?;
    Ok(())
  }
}

struct TypeResolutionContext<'a> {
  unresolved: HashMap<&'a str, &'a ast::TypeItem<'a>>,
  resolved: BTreeMap<Arc<str>, SpecializedType>,
}

impl<'a> TypeResolutionContext<'a> {
  fn new(schema: &ast::Schema<'a>) -> Result<Self> {
    let mut types: HashMap<&'a str, &'a ast::TypeItem<'a>> = HashMap::new();
    for item in &schema.items {
      match item {
        ast::SchemaItem::Type(x) => {
          if types.contains_key(x.name.0) {
            return Err(SchemaCompileError::DuplicateType(x.name.0.to_string()).into());
          }
          types.insert(x.name.0, x);
        }
        _ => {}
      }
    }
    Ok(Self {
      unresolved: types,
      resolved: BTreeMap::new(),
    })
  }

  fn resolve_type_expr(
    &mut self,
    local_context: &HashMap<&'a str, &FieldType>,
    e: &TypeExpr<'a>,
  ) -> Result<FieldType> {
    let (id, args) = match e {
      TypeExpr::Unit(x) => (x, &[] as _),
      TypeExpr::Specialize(x, args) => (x, args.as_slice()),
    };

    let args = args
      .iter()
      .map(|x| self.resolve_type_expr(local_context, x))
      .collect::<Result<Vec<_>>>()?;

    // If this type is in its local context (type parameters of the type), return it.
    if let Some(&x) = local_context.get(id.0) {
      if args.len() != 0 {
        return Err(SchemaCompileError::CannotSpecializeTypeParameter(id.0.to_string()).into());
      }
      return Ok(x.clone());
    }

    // If this type is a primitive type...
    if let Some(ty) = PRIMITIVE_TYPES.get(id.0) {
      if args.len() != 0 {
        return Err(SchemaCompileError::CannotSpecializePrimitiveType(id.0.to_string()).into());
      }
      return Ok(FieldType::Primitive(*ty));
    }

    // The only special case, `set`...
    if id.0 == "set" {
      if args.len() != 1 {
        return Err(SchemaCompileError::BadSetTypeParameter.into());
      }
      return Ok(FieldType::Set(Box::new(args[0].clone())));
    }

    let ty = self
      .unresolved
      .get(id.0)
      .copied()
      .ok_or_else(|| SchemaCompileError::MissingType(id.0.to_string()))?;
    if ty.generics.len() != args.len() {
      return Err(
        SchemaCompileError::ArgCountMismatch {
          expected_args: ty.generics.len(),
          ty: id.0.to_string(),
          got_args: args.len(),
        }
        .into(),
      );
    }

    let repr = Arc::from(format!(
      "{}<{}>",
      id.0,
      args
        .iter()
        .map(|x| format!("{}", x))
        .collect::<Vec<_>>()
        .join(", "),
    ));

    // Now we have the type itself, let's look at the fields.
    // If the type is already resolved, use it.
    if self.resolved.contains_key(&repr) {
      return Ok(FieldType::Named(repr));
    }

    // Not yet resolved: let's resolve it.
    // Insert with empty fields; fill the actual types in later.
    // This allows us to have recursive types.
    self.resolved.insert(
      repr.clone(),
      SpecializedType {
        name: repr.clone(),
        fields: BTreeMap::new(),
      },
    );

    // Construct a new local context: specialized types of the type parameters.
    let local_context: HashMap<&'a str, &FieldType> =
      ty.generics.iter().map(|x| x.0).zip(args.iter()).collect();

    // Then, recursively resolve the types of fields.
    let mut fields: BTreeMap<Arc<str>, (FieldType, Vec<FieldAnnotation>)> = BTreeMap::new();
    for x in &ty.fields {
      if fields.contains_key(x.name.0) {
        return Err(
          SchemaCompileError::DuplicateField {
            field: x.name.0.to_string(),
            ty: ty.name.0.to_string(),
          }
          .into(),
        );
      }
      let mut field_ty = self.resolve_type_expr(&local_context, &x.value)?;
      if x.optional {
        field_ty = FieldType::Optional(Box::new(field_ty));
      }

      let mut annotations = vec![];
      for ann in &x.annotations {
        match (ann.name.0, ann.args.as_slice()) {
          ("unique", []) => {
            annotations.push(FieldAnnotation::Unique);
          }
          ("index", []) => {
            annotations.push(FieldAnnotation::Index);
          }
          ("packed", []) => {
            annotations.push(FieldAnnotation::Packed);
          }
          ("rename_from", [Literal::String(x)]) => {
            annotations.push(FieldAnnotation::RenameFrom(x.to_string()));
          }
          _ => {
            return Err(
              SchemaCompileError::UnknownAnnotationOnField(
                x.name.0.to_string(),
                repr.to_string(),
                ann.name.0.to_string(),
              )
              .into(),
            )
          }
        }
      }

      // Validate index constraints.
      //
      // Currently, a unique/non-unique index is only allowed on either packed or primitive fields.
      if annotations
        .iter()
        .find(|x| x.is_unique() || x.is_index())
        .is_some()
      {
        match field_ty.optional_unwrapped() {
          FieldType::Primitive(_) => {}
          _ => {
            if annotations.iter().find(|x| x.is_packed()).is_none() {
              return Err(
                SchemaCompileError::IndexOnNonPrimitiveOrPackedField(
                  x.name.0.to_string(),
                  ty.name.0.to_string(),
                )
                .into(),
              );
            }
          }
        }
      }
      fields.insert(Arc::from(x.name.0), (field_ty, annotations));
    }

    self.resolved.get_mut(&repr).unwrap().fields = fields;

    Ok(FieldType::Named(repr))
  }
}
