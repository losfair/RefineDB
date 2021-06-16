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
