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
    some_item = get_field(some_item) root;
    id = get_field(id) some_item;
    name = get_field(name) some_item;

    dur = get_field(duration) some_item;
    if eq const("test") name {
      v1 = get_field(start) dur;
      k1 = const("start");
    } else {
      v2 = get_field(end) dur;
      k2 = const("end");
    }
    value = select v1 v2;
    kind = select k1 k2;

    s = get_field(many_items) root;
    elem_name_1 = get_field(name) (point_get const("xxx") s);
    elem_name_2 = get_field(name) (point_get const("yyy") s);
    elem_name_3 = get_field(name)(point_get const("zzz") s);

    return m_insert(id) id
      $ m_insert(name) name
      $ m_insert(value) value
      $ m_insert(kind) kind
      $ m_insert(set_member_name_1) elem_name_1
      $ m_insert(set_member_name_2) elem_name_2
      $ m_insert(set_member_name_3) elem_name_3
      create_map;
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
      some_item = get_field(some_item) root;
      start = const(1);
      end = const(2);
      m = create_map;
      m = m_insert(start) start m;
      m = m_insert(end) end m;
      dur = build_table(Duration<int64>) m;
      t_insert(duration) dur some_item;

      id = const("test_id");
      name = const("test_name");
      t_insert(id) id some_item;
      t_insert(name) name some_item;
    }
    "#,
      READER,
      r#"
  graph main(root: schema) {
    some_item = get_field(some_item) root;
    name = const("test");
    t_insert(name) name some_item;

    start = const(1);
    end = const(2);
    m = create_map;
    m = m_insert(start) start m;
    m = m_insert(end) end m;
    dur = build_table(Duration<int64>) m;

    elem = create_map;
    v = const("xxx");
    elem = m_insert(id) v elem;
    v = const("name_for_xxx");
    elem = m_insert(name) v elem;
    elem = m_insert(duration) dur elem;
    elem = build_table(Item<>) elem;
    s = get_field(many_items) root;
    s_insert elem s;

    elem = create_map;
    v = const("yyy");
    elem = m_insert(id) v elem;
    v = const("name_for_yyy");
    elem = m_insert(name) v elem;
    elem = m_insert(duration) dur elem;
    elem = build_table(Item<>) elem;
    s = get_field(many_items) root;
    s_insert elem s;
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
