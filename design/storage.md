# Storage

## Types

RefineDB has three classes of types:

- Primitives

`int64`, `double`, `bytes`, `string`

- Sets

Each *set* contains many *tables* of the same type with a primary key.

- Tables

A *table* is similar to a *struct* in other programming languages.

Tables can be recursive.

```
type SomeTable {
  field_1: int64,
  field_2: string,
  recursive: SomeTable?,
}
```

## Exports

Data in a schema is organized as a tree. The root node is the schema itself, and *exports*
are first-level child nodes of the root node.

```
export SomeTable t;
export set<SomeTable> s;
```

## Storage plan

A storage plan is an assignment of keyspaces to nodes, and is generated automatically from
the schema on initial creation and migration.

An example storage plan for the following schema:

```
type Item<T> {
  @primary
  id: string,
  value: T,
}
type RecursiveItem<T> {
  @primary
  id: string,
  value: T?,
  recursive: RecursiveItem<T>?,
}
type Duration<T> {
  start: T,
  end: T,
}
export set<Item<Duration<int64>>> items;
export set<RecursiveItem<Duration<int64>>> recursive_items;
```

is:

```yaml
nodes:
  items:
    key: AXosgWZFums2K/zU
    flattened: false
    subspace_reference: false
    packed: false
    set:
      key: AXosgWZFL4sleLmR
      flattened: true
      subspace_reference: false
      packed: false
      set: ~
      children:
        id:
          key: AXosgWZF0K28ukry
          flattened: false
          subspace_reference: false
          packed: false
          set: ~
          children: {}
        value:
          key: AXosgWZF5u3TGalq
          flattened: true
          subspace_reference: false
          packed: false
          set: ~
          children:
            end:
              key: AXosgWZFwjhc/Tpj
              flattened: false
              subspace_reference: false
              packed: false
              set: ~
              children: {}
            start:
              key: AXosgWZF+1CAHgVF
              flattened: false
              subspace_reference: false
              packed: false
              set: ~
              children: {}
    children: {}
  recursive_items:
    key: AXosgWZFfTuTFdSE
    flattened: false
    subspace_reference: false
    packed: false
    set:
      key: AXosgWZFUZV7X+pU
      flattened: false
      subspace_reference: false
      packed: false
      set: ~
      children:
        id:
          key: AXosgWZFM4IM9jIh
          flattened: false
          subspace_reference: false
          packed: false
          set: ~
          children: {}
        recursive:
          key: AXosgWZFUZV7X+pU
          flattened: false
          subspace_reference: true
          packed: false
          set: ~
          children: {}
        value:
          key: AXosgWZFBVTr65Uo
          flattened: true
          subspace_reference: false
          packed: false
          set: ~
          children:
            end:
              key: AXosgWZFGLHaEVTG
              flattened: false
              subspace_reference: false
              packed: false
              set: ~
              children: {}
            start:
              key: AXosgWZFkVcxMnO+
              flattened: false
              subspace_reference: false
              packed: false
              set: ~
              children: {}
    children: {}
```

The storage plan specifies the paths to all the fields reachable from exports. For example:

```
items -> [AXosgWZFums2K/zU]
items[id == "test"] -> [AXosgWZFums2K/zU] [<set_key>] [AXosgWZFL4sleLmR]
items[id == "test"].id -> [AXosgWZFums2K/zU] [<set_key>] [AXosgWZF0K28ukry]
items[id == "test"].value -> [AXosgWZFums2K/zU] [<set_key>] [AXosgWZF5u3TGalq]
items[id == "test"].value.start -> [AXosgWZFums2K/zU] [<set_key>] [AXosgWZF+1CAHgVF]
items[id == "test"].value.end -> [AXosgWZFums2K/zU] [<set_key>] [AXosgWZFwjhc/Tpj]

recursive_items -> [AXosgWZFfTuTFdSE]
items[id == "test"] -> [AXosgWZFfTuTFdSE] [<set_key>] [AXosgWZFUZV7X+pU]
items[id == "test"].id -> [AXosgWZFfTuTFdSE] [<set_key>] [AXosgWZFUZV7X+pU] [AXosgWZFM4IM9jIh]
items[id == "test"].value -> [AXosgWZFfTuTFdSE] [<set_key>] [AXosgWZFUZV7X+pU] [AXosgWZFBVTr65Uo]
items[id == "test"].value.start -> [AXosgWZFfTuTFdSE] [<set_key>] [AXosgWZFUZV7X+pU] [AXosgWZFkVcxMnO+]
items[id == "test"].value.end -> [AXosgWZFfTuTFdSE] [<set_key>] [AXosgWZFUZV7X+pU] [AXosgWZFGLHaEVTG]
items[id == "test"].recursive -> [AXosgWZFfTuTFdSE] [<set_key>] [AXosgWZFUZV7X+pU] [AXosgWZFUZV7X+pU]
items[id == "test"].recursive.id -> [AXosgWZFfTuTFdSE] [<set_key>] [AXosgWZFUZV7X+pU] [AXosgWZFUZV7X+pU] [AXosgWZFM4IM9jIh]
items[id == "test"].recursive.value -> [AXosgWZFfTuTFdSE] [<set_key>] [AXosgWZFUZV7X+pU] [AXosgWZFUZV7X+pU] [AXosgWZFBVTr65Uo]
items[id == "test"].recursive.value.start -> [AXosgWZFfTuTFdSE] [<set_key>] [AXosgWZFUZV7X+pU] [AXosgWZFUZV7X+pU] [AXosgWZFkVcxMnO+]
items[id == "test"].recursive.value.end -> [AXosgWZFfTuTFdSE] [<set_key>] [AXosgWZFUZV7X+pU] [AXosgWZFUZV7X+pU] [AXosgWZFGLHaEVTG]
```

The *set key* is derived from the value of the primary key of the member table type.

Note that fields are *flattened* when doing so does not lead to ambiguity: `.value.start` and
`.value.end` have the same key length as `.value` and `.id`. Currently two kinds of types are
not flattened, for obvious reasons:

- Sets.
- Recursive tables.
