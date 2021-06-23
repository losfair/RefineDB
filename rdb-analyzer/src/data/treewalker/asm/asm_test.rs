use std::{sync::Arc, time::Instant};

use bumpalo::Bump;

use crate::{
  data::{
    fixup::migrate_schema,
    mock_kv::MockKv,
    treewalker::{
      asm::codegen::compile_twscript,
      exec::{generate_root_map, Executor},
      typeck::GlobalTyckContext,
      vm::TwVm,
      vm_value::VmValue,
    },
    value::PrimitiveValue,
  },
  schema::{compile::compile, grammar::parse},
  storage_plan::planner::generate_plan_for_schema,
};

async fn simple_test<F: FnMut(Option<Arc<VmValue>>)>(schema: &str, scripts: &[&str], mut check: F) {
  let alloc = Bump::new();
  let ast = parse(&alloc, schema).unwrap();
  let schema = compile(&ast).unwrap();
  drop(ast);
  drop(alloc);
  let plan = generate_plan_for_schema(&Default::default(), &Default::default(), &schema).unwrap();

  let kv = MockKv::new();
  migrate_schema(&schema, &plan, &kv).await.unwrap();

  for &code in scripts {
    let start = Instant::now();
    let script = compile_twscript(code).unwrap();
    let compile_end = Instant::now();
    println!("compile took {:?}", compile_end.duration_since(start));

    println!("{:?}", script);
    let vm = TwVm::new(&schema, &plan, &script).unwrap();

    let start = Instant::now();
    GlobalTyckContext::new(&vm).unwrap().typeck().unwrap();
    let tyck_end = Instant::now();
    println!("tyck took {:?}", tyck_end.duration_since(start));

    let executor = Executor::new_assume_typechecked(&vm, &kv);
    let output = executor
      .run_graph(0, &[Arc::new(generate_root_map(&schema, &plan).unwrap())])
      .await
      .unwrap();
    let exec_end = Instant::now();
    println!("exec took {:?}", exec_end.duration_since(tyck_end));
    println!("{:?}", output);
    check(output);
  }
}

#[tokio::test]
async fn basic_exec() {
  const READER: &str = r#"
  graph main(root: schema): map {
    id: string,
    name: string,
    value: int64,
    kind: string,
    set_member_name_1: string,
    set_member_name_2: string,
  } {
    root = param(root);
    some_item = get_field(some_item) root;
    id = get_field(id) some_item;
    name = get_field(name) some_item;

    expected_name = const("test");
    name_matches = eq expected_name name;
    dur = get_field(duration) some_item;
    if name_matches {
      v1 = get_field(start) dur;
      k1 = const("start");
    } else {
      v2 = get_field(end) dur;
      k2 = const("end");
    }
    value = select v1 v2;
    kind = select k1 k2;

    s = get_field(many_items) root;
    expected_id = const("xxx");
    elem = point_get expected_id s;
    elem_name_1 = get_field(name) elem;
    expected_id = const("yyy");
    elem = point_get expected_id s;
    elem_name_2 = get_field(name) elem;
    expected_id = const("zzz");
    elem = point_get expected_id s;
    elem_name_3 = get_field(name) elem;

    m = create_map;
    m = insert_into_map(id) id m;
    m = insert_into_map(name) name m;
    m = insert_into_map(value) value m;
    m = insert_into_map(kind) kind m;
    m = insert_into_map(set_member_name_1) elem_name_1 m;
    m = insert_into_map(set_member_name_2) elem_name_2 m;
    m = insert_into_map(set_member_name_3) elem_name_3 m;
    return m;
  }
  "#;
  let _ = pretty_env_logger::try_init();
  let mut chkindex = 0usize;
  simple_test(
    r#"
  type Item {
    @primary
    id: string,
    name: string,
    duration: Duration<int64>,
  }
  type Duration<T> {
    start: T,
    end: T,
  }
  export Item some_item;
  export set<Item> many_items;
  "#,
    &[
      r#"
    graph main(root: schema) {
      root = param(root);
      some_item = get_field(some_item) root;
      start = const(1);
      end = const(2);
      m = create_map;
      m = insert_into_map(start) start m;
      m = insert_into_map(end) end m;
      dur = build_table(Duration<int64>) m;
      insert_into_table(duration) dur some_item;

      id = const("test_id");
      name = const("test_name");
      insert_into_table(id) id some_item;
      insert_into_table(name) name some_item;
    }
    "#,
      READER,
      r#"
  graph main(root: schema) {
    root = param(root);
    some_item = get_field(some_item) root;
    name = const("test");
    insert_into_table(name) name some_item;

    start = const(1);
    end = const(2);
    m = create_map;
    m = insert_into_map(start) start m;
    m = insert_into_map(end) end m;
    dur = build_table(Duration<int64>) m;

    elem = create_map;
    v = const("xxx");
    elem = insert_into_map(id) v elem;
    v = const("name_for_xxx");
    elem = insert_into_map(name) v elem;
    elem = insert_into_map(duration) dur elem;
    elem = build_table(Item<>) elem;
    s = get_field(many_items) root;
    insert_into_set elem s;

    elem = create_map;
    v = const("yyy");
    elem = insert_into_map(id) v elem;
    v = const("name_for_yyy");
    elem = insert_into_map(name) v elem;
    elem = insert_into_map(duration) dur elem;
    elem = build_table(Item<>) elem;
    s = get_field(many_items) root;
    insert_into_set elem s;
  }
  "#,
      READER,
    ],
    |x| {
      match chkindex {
        0 => {}
        1 => {
          let x = x.unwrap();
          let x = x.unwrap_map();
          assert_eq!(
            x.elements.get("id").unwrap().unwrap_primitive(),
            &PrimitiveValue::String("test_id".into())
          );
          assert_eq!(
            x.elements.get("name").unwrap().unwrap_primitive(),
            &PrimitiveValue::String("test_name".into())
          );
          assert_eq!(
            x.elements.get("value").unwrap().unwrap_primitive(),
            &PrimitiveValue::Int64(2)
          );
          assert_eq!(
            x.elements.get("kind").unwrap().unwrap_primitive(),
            &PrimitiveValue::String("end".into())
          );
          assert_eq!(
            **x.elements.get("set_member_name_1").unwrap(),
            VmValue::Null,
          );
          assert_eq!(
            **x.elements.get("set_member_name_2").unwrap(),
            VmValue::Null,
          );
          assert_eq!(
            **x.elements.get("set_member_name_3").unwrap(),
            VmValue::Null,
          );
        }
        2 => {}
        3 => {
          let x = x.unwrap();
          let x = x.unwrap_map();
          assert_eq!(
            x.elements.get("id").unwrap().unwrap_primitive(),
            &PrimitiveValue::String("test_id".into())
          );
          assert_eq!(
            x.elements.get("name").unwrap().unwrap_primitive(),
            &PrimitiveValue::String("test".into())
          );
          assert_eq!(
            x.elements.get("value").unwrap().unwrap_primitive(),
            &PrimitiveValue::Int64(1)
          );
          assert_eq!(
            x.elements.get("kind").unwrap().unwrap_primitive(),
            &PrimitiveValue::String("start".into())
          );
          assert_eq!(
            x.elements
              .get("set_member_name_1")
              .unwrap()
              .unwrap_primitive(),
            &PrimitiveValue::String("name_for_xxx".into())
          );
          assert_eq!(
            x.elements
              .get("set_member_name_2")
              .unwrap()
              .unwrap_primitive(),
            &PrimitiveValue::String("name_for_yyy".into())
          );
          assert_eq!(
            **x.elements.get("set_member_name_3").unwrap(),
            VmValue::Null,
          );
        }
        _ => unreachable!(),
      }
      chkindex += 1;
    },
  )
  .await;
  assert_eq!(chkindex, 4);
}
