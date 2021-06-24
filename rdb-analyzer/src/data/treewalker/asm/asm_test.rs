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
    let type_info = GlobalTyckContext::new(&vm).unwrap().typeck().unwrap();
    let tyck_end = Instant::now();
    println!("tyck took {:?}", tyck_end.duration_since(start));

    let executor = Executor::new(&vm, &kv, &type_info);
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
    altname: string,
    value: int64,
    kind: string,
    set_member_name_1: string,
    set_member_name_2: string,
    set_member_name_3: string,
    set_member_name_1_nonnull: string,
  } {
    some_item = root.some_item;
    id = some_item.id;
    name = some_item.name;

    dur = some_item.duration;
    if name == "test" {
      v1 = dur.start;
      k1 = "start";
    } else {
      v2 = dur.end;
      k2 = "end";
    }
    value = select v1 v2;
    kind = select k1 k2;

    s = root.many_items;
    elem_name_1 = (point_get s "xxx").name;
    elem_name_2 = (point_get s "yyy").name;
    elem_name_3 = (point_get s "zzz").name;

    return m_insert(id) id
      $ m_insert(name) name
      $ m_insert(value) value
      $ m_insert(kind) kind
      $ m_insert(altname) some_item.altname
      $ m_insert(set_member_name_1) elem_name_1
      $ m_insert(set_member_name_2) elem_name_2
      $ m_insert(set_member_name_3) elem_name_3
      $ m_insert(set_member_name_1_nonnull) (elem_name_1 ?? "<unknown>")
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
    @default("hello")
    altname: string,
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
      some_item = root.some_item;
      t_insert(duration) some_item $
        build_table(Duration<int64>) $
        m_insert(start) 1 $
        m_insert(end) 2 $
        create_map;
      t_insert(id) some_item "test_id";
      t_insert(name) some_item "test_name";
    }
    "#,
      READER,
      r#"
  graph main(root: schema) {
    t_insert(name) root.some_item "test";

    m = m_insert(start) 1 $ m_insert(end) 2 $ create_map;
    dur = build_table(Duration<int64>) m;

    s_insert root.many_items $ build_table(Item)
      $ m_insert(id) "xxx"
      $ m_insert(name) "name_for_xxx"
      $ m_insert(altname) "testalt"
      $ m_insert(duration) dur
      $ create_map;
    s_insert root.many_items $ build_table(Item)
      $ m_insert(id) "yyy"
      $ m_insert(name) "name_for_yyy"
      $ m_insert(altname) "testalt"
      $ m_insert(duration) dur
      $ create_map;
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
            x.elements.get("altname").unwrap().unwrap_primitive(),
            &PrimitiveValue::String("hello".into())
          );
          assert_eq!(
            x.elements.get("value").unwrap().unwrap_primitive(),
            &PrimitiveValue::Int64(2)
          );
          assert_eq!(
            x.elements.get("kind").unwrap().unwrap_primitive(),
            &PrimitiveValue::String("end".into())
          );
          assert!(x.elements.get("set_member_name_1").unwrap().is_null());
          assert!(x.elements.get("set_member_name_2").unwrap().is_null());
          assert!(x.elements.get("set_member_name_3").unwrap().is_null());
          assert_eq!(
            x.elements
              .get("set_member_name_1_nonnull")
              .unwrap()
              .unwrap_primitive(),
            &PrimitiveValue::String("<unknown>".into())
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
          assert!(x.elements.get("set_member_name_3").unwrap().is_null());
          assert_eq!(
            x.elements
              .get("set_member_name_1_nonnull")
              .unwrap()
              .unwrap_primitive(),
            &PrimitiveValue::String("name_for_xxx".into())
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
