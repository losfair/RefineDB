use std::{sync::Arc, time::Instant};

use anyhow::Result;
use bumpalo::Bump;

use crate::{
  data::{
    treewalker::{
      asm::codegen::compile_twscript,
      exec::{generate_root_map, Executor},
      serialize::{SerializedVmValue, TaggedVmValue},
      typeck::GlobalTyckContext,
      vm::TwVm,
      vm_value::{VmType, VmValue},
    },
    value::PrimitiveValue,
  },
  schema::{
    compile::{compile, PrimitiveType},
    grammar::parse,
  },
  storage_plan::planner::generate_plan_for_schema,
  test_util::create_kv,
};

async fn simple_test_with_error<F: FnMut(Result<Option<Arc<VmValue>>>)>(
  schema: &str,
  scripts: &[&str],
  mut check: F,
) {
  let alloc = Bump::new();
  let ast = parse(&alloc, schema).unwrap();
  let schema = compile(&ast).unwrap();
  drop(ast);
  drop(alloc);
  let plan = generate_plan_for_schema(&Default::default(), &Default::default(), &schema).unwrap();

  let kv = create_kv();

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

    let mut executor = Executor::new(&vm, &*kv, &type_info);
    let output = executor
      .run_graph(0, &[Arc::new(generate_root_map(&schema, &plan).unwrap())])
      .await;
    let exec_end = Instant::now();
    println!("exec took {:?}", exec_end.duration_since(tyck_end));
    println!("{:?}", output);
    check(output);
  }
}

async fn simple_test<F: FnMut(Option<Arc<VmValue>>)>(schema: &str, scripts: &[&str], mut check: F) {
  simple_test_with_error(schema, scripts, |x| check(x.unwrap())).await
}

#[tokio::test]
async fn basic_exec() {
  const READER: &str = r#"
  type SomeString = string;
  graph main(root: schema): map {
    id: string,
    name: string,
    altname: string,
    value: int64,
    kind: string,
    set_member_name_1: string,
    set_member_name_2: string,
    set_member_name_3: string,
    set_member_name_1_nonnull: SomeString,
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
            x.elements.get("name").unwrap().unwrap_primitive(),
            &PrimitiveValue::String("test_name".into())
          );
          assert!(matches!(
            &**x.elements.get("altname").unwrap(),
            VmValue::Null(VmType::Primitive(PrimitiveType::String)),
          ),);
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

#[tokio::test]
async fn fib() {
  let _ = pretty_env_logger::try_init();
  let mut ok = false;
  simple_test(
    r#"
  "#,
    &[r#"
    graph main(root: schema): int64 {
      return call(fib) [20];
    }
    graph fib(x: int64): int64 {
      if x == 1 || x == 2 {
        v1 = 1;
      } else {
        v2 = call(fib) [x - 1] + call(fib) [x - 2];
      }
      return select v1 v2;
    }
    "#],
    |x| {
      assert_eq!(
        **x.as_ref().unwrap(),
        VmValue::Primitive(PrimitiveValue::Int64(6765))
      );
      ok = true;
    },
  )
  .await;

  assert!(ok);
}

#[tokio::test]
async fn set_reduce() {
  const READER: &str = r#"
  graph main(root: schema): string {
    return (reduce(f) create_map (
      m_insert(first) true $
      m_insert(result) "" create_map
    ) root.items).result;
  }
  graph f(ctx: map{}, current: map {
    first: bool,
    result: string,
  }, item: Item): map {
    first: bool,
    result: string,
  } {
    if !current.first {
      r1 = current.result + " " + item.id;
    } else {
      r2 = item.id;
    }
    return m_insert(first) false
      $ m_insert(result) (select r1 r2)
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
  }
  export set<Item> items;
  "#,
    &[
      READER,
      r#"
      graph main(root: schema) {
        s_insert root.items $ build_table(Item)
          $ m_insert(id) "id1" create_map;
        s_insert root.items $ build_table(Item)
          $ m_insert(id) "id2" create_map;
        s_insert root.items $ build_table(Item)
          $ m_insert(id) "id3" create_map;
        s_insert root.items $ build_table(Item)
          $ m_insert(id) "id4" create_map;
      }
      "#,
      READER,
      r#"
      graph main(root: schema): string {
        return (reduce(f) from "id2" to "id4" create_map (
          m_insert(first) true $
          m_insert(result) "" create_map
        ) root.items).result;
      }
      graph f(ctx: map{}, current: map {
        first: bool,
        result: string,
      }, item: Item): map {
        first: bool,
        result: string,
      } {
        if !current.first {
          r1 = current.result + " " + item.id;
        } else {
          r2 = item.id;
        }
        return m_insert(first) false
          $ m_insert(result) (select r1 r2)
          create_map;
      }
      "#,
    ],
    |x| {
      match chkindex {
        0 => {
          assert_eq!(
            **x.as_ref().unwrap(),
            VmValue::Primitive(PrimitiveValue::String("".into()))
          );
        }
        1 => {}
        2 => {
          assert_eq!(
            **x.as_ref().unwrap(),
            VmValue::Primitive(PrimitiveValue::String("id1 id2 id3 id4".into()))
          );
        }
        3 => {
          assert_eq!(
            **x.as_ref().unwrap(),
            VmValue::Primitive(PrimitiveValue::String("id2 id3".into()))
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

#[tokio::test]
async fn list_ops() {
  let _ = pretty_env_logger::try_init();
  let mut chkindex = 0usize;
  simple_test(
    r#"
    type Store {
      collection_a: set<Number>,
      collection_b: set<Number>,
    }
    type Number {
      @primary
      value: int64,
    }
    export Store store;
  "#,
    &[r#"
    graph main(root: schema): map {
      list1: list<int64>,
      sum1: int64,
      list2: list<int64>,
      sum2: int64,
    } {
      list1 = 5 : 4 : 3 : 2 : 1 : create_list(int64);
      sum1 = reduce(sum) create_map 0 list1;

      list2 = call(gen_numbers) [20];
      sum2 = reduce(sum) create_map 0 list2;

      ret = m_insert(list1) list1
        $ m_insert(sum1) sum1
        $ m_insert(list2) list2
        $ m_insert(sum2) sum2
        $ create_map;
      t_insert(collection_a) root.store $ build_set $ reduce(transform_numbers) create_map create_list(Number) list1;
      t_insert(collection_b) root.store $ build_set $ reduce(transform_numbers) create_map create_list(Number) list2;
      return ret;
    }

    graph transform_numbers(_unused: map{}, current: list<Number>, v: int64): list<Number> {
      return (build_table(Number) $ m_insert(value) v create_map) : current;
    }

    graph gen_numbers(n: int64): list<int64> {
      if n == 0 {
        r1 = create_list(int64);
      } else {
        r2 = n : call(gen_numbers) [n - 1];
      }
      return select r1 r2;
    }

    graph sum(_unused: map{}, current: int64, that: int64): int64 {
      return current + that;
    }
    "#, r#"
    graph main(root: schema): map {
      list1: list<int64>,
      list2: list<int64>,
    } {
      list1 = reduce(decompose_numbers) create_map create_list(int64) root.store.collection_a;
      list2 = reduce(decompose_numbers) create_map create_list(int64) root.store.collection_b;
      return m_insert(list1) list1
        $ m_insert(list2) list2
        create_map;
    }
    graph decompose_numbers(_unused: map{}, current: list<int64>, v: Number): list<int64> {
      return v.value : current;
    }
    "#],
    |x| {
      match chkindex {
        0 => match &**x.as_ref().unwrap() {
          VmValue::Map(x) => {
            assert_eq!(**x.elements.get("sum1").unwrap(), VmValue::Primitive(PrimitiveValue::Int64(15)));
            assert_eq!(**x.elements.get("sum2").unwrap(), VmValue::Primitive(PrimitiveValue::Int64(210)));
          }
          _ => unreachable!(),
        },
        1 => {
          let serialized = SerializedVmValue::encode(&**x.as_ref().unwrap(), &Default::default()).unwrap();
          match &serialized {
            SerializedVmValue::Tagged(TaggedVmValue::M(x)) => {
              match x.get("list1").unwrap() {
                SerializedVmValue::Tagged(TaggedVmValue::L(x)) => assert_eq!(x.len(), 5),
                _ => unreachable!(),
              }
              match x.get("list2").unwrap() {
                SerializedVmValue::Tagged(TaggedVmValue::L(x)) => assert_eq!(x.len(), 20),
                _ => unreachable!(),
              }
            }
            _ => unreachable!(),
          }
        }
        _ => unreachable!(),
      }
      chkindex += 1;
    },
  )
  .await;

  assert_eq!(chkindex, 2);
}

#[tokio::test]
async fn throw_string() {
  let _ = pretty_env_logger::try_init();
  let mut ok = false;
  simple_test_with_error(
    r#"
  "#,
    &[r#"
    graph main(root: schema) {
      throw "test error";
    }
    "#],
    |x| {
      assert_eq!(
        x.unwrap_err().to_string(),
        "script thrown error: `test error`"
      );
      ok = true;
    },
  )
  .await;

  assert!(ok);
}

#[tokio::test]
async fn throw_null() {
  let _ = pretty_env_logger::try_init();
  let mut ok = false;
  simple_test_with_error(
    r#"
  "#,
    &[r#"
    graph main(root: schema) {
      throw null<string>;
    }
    "#],
    |x| {
      assert_eq!(x.unwrap_err().to_string(), "script thrown null");
      ok = true;
    },
  )
  .await;

  assert!(ok);
}
