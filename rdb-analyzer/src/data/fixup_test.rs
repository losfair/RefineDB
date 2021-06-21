use bumpalo::Bump;

use crate::{
  schema::{compile::compile, grammar::parse},
  storage_plan::planner::generate_plan_for_schema,
};

use super::{fixup::migrate_schema, mock_kv::MockKv};

#[tokio::test]
async fn fixup_migration_add_field() {
  let _ = pretty_env_logger::try_init();
  let old = r#"
  type Item {
    @primary
    a: int64,
    b: set<Item>,
    c: bytes,
  }
  type BinaryTree<T> {
    left: BinaryTree<T>?,
    right: BinaryTree<T>?,
    value: T?,
  }
  export Item data;
  export BinaryTree<int64> bt;
  "#;
  let new = r#"
  type Item {
    @primary
    a: int64,
    b: set<Item>,
    c: bytes,
    d: string,
  }
  export Item data;
  "#;

  let alloc = Bump::new();
  let ast = parse(&alloc, old).unwrap();
  let schema1 = compile(&ast).unwrap();
  drop(ast);
  drop(alloc);
  let plan1 = generate_plan_for_schema(&Default::default(), &Default::default(), &schema1).unwrap();

  let alloc = Bump::new();
  let ast = parse(&alloc, new).unwrap();
  let schema2 = compile(&ast).unwrap();
  drop(ast);
  drop(alloc);

  let plan2 = generate_plan_for_schema(&plan1, &schema1, &schema2).unwrap();

  let kv = MockKv::new();
  let version_0 = kv.dump().await;
  migrate_schema(&schema1, &plan1, &kv).await.unwrap();
  let version_1 = kv.dump().await;
  migrate_schema(&schema2, &plan2, &kv).await.unwrap();
  let version_2 = kv.dump().await;
  migrate_schema(&schema2, &plan2, &kv).await.unwrap();
  let version_3 = kv.dump().await;
  assert!(version_0 != version_1);
  assert!(version_1 != version_2);
  assert!(version_2 == version_3);
}
