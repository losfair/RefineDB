use bumpalo::Bump;

use super::{compile::compile, grammar::parse};

#[test]
fn test_compile_simple() {
  let _ = pretty_env_logger::try_init();
  let alloc = Bump::new();
  let ast = parse(
    &alloc,
    r#"
    type Item<T> {
      @packed inner: T,
      something_else: string,
    }
    type Duration<T> {
      start: T,
      end: T,
    }
    type Recursive<T> {
      inner: Recursive<T>?,
    }
    export set<Item<Duration<int64>>> items;
    export Recursive<int64> item;
  "#,
  )
  .unwrap();
  let output = compile(&ast).unwrap();
  println!("{}", output);
}

#[test]
fn index_constraints_case_1() {
  let _ = pretty_env_logger::try_init();
  let alloc = Bump::new();
  let ast = parse(
    &alloc,
    r#"
    type Item<T> {
      @unique key1: T,
      @packed @unique key2: Wrapped<T>,
      @unique key3: T?,
      @packed @unique key4: Wrapped<T>?,
    }
    type Wrapped<T> {
      inner: T,
    }
    export Item<int64> item;
  "#,
  )
  .unwrap();
  let output = compile(&ast).unwrap();
  println!("{}", output);
}

#[test]
fn index_constraints_case_2() {
  let _ = pretty_env_logger::try_init();
  let alloc = Bump::new();
  let ast = parse(
    &alloc,
    r#"
    type Item<T> {
      @unique key1: T,
      @unique key2: Wrapped<T>,
    }
    type Wrapped<T> {
      inner: T,
    }
    export Item<int64> item;
  "#,
  )
  .unwrap();
  assert!(compile(&ast).is_err());
}

#[test]
fn no_primitive_types_in_set() {
  let _ = pretty_env_logger::try_init();
  let alloc = Bump::new();
  let ast = parse(
    &alloc,
    r#"
    export set<int64> something;
  "#,
  )
  .unwrap();
  assert!(compile(&ast).is_err());
}

#[test]
fn primary_keys() {
  let _ = pretty_env_logger::try_init();
  let alloc = Bump::new();
  let ast = parse(
    &alloc,
    r#"
    type Item<T> {
      @primary key: T,
    }
    export Item<int64> something;
  "#,
  )
  .unwrap();
  compile(&ast).unwrap();
}

#[test]
fn primary_keys_cannot_be_optional() {
  let _ = pretty_env_logger::try_init();
  let alloc = Bump::new();
  let ast = parse(
    &alloc,
    r#"
    type Item<T> {
      @primary key: T?,
    }
    export Item<int64> something;
  "#,
  )
  .unwrap();
  assert!(compile(&ast)
    .unwrap_err()
    .to_string()
    .contains("is a primary key and cannot be optional"));
}

#[test]
fn at_most_one_primary_key() {
  let _ = pretty_env_logger::try_init();
  let alloc = Bump::new();
  let ast = parse(
    &alloc,
    r#"
    type Item<T> {
      @primary key1: T,
      @primary key2: T,
    }
    export Item<int64> something;
  "#,
  )
  .unwrap();
  assert!(compile(&ast)
    .unwrap_err()
    .to_string()
    .contains("has multiple primary keys"));
}
