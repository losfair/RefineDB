use bumpalo::Bump;

use super::{compile::compile, grammar::parse};

#[test]
fn test_compile_simple() {
  let alloc = Bump::new();
  let ast = parse(
    &alloc,
    r#"
    type Item<T> {
      inner: T,
    }
    type Empty {}
    export Item<Empty> item;
  "#,
  )
  .unwrap();
  let output = compile(&ast).unwrap();
  println!("{}", output);
}
