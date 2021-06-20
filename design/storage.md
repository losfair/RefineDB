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
  inner: T,
  @primary
  id: string,
}
type Duration<T> {
  start: T,
  end: T,
}
export set<Item<Duration<int64>>> items;
```

is:

```yaml
items:
  key: AXopPNuD3RsfWb7N
  flattened: false
  subspace_reference: false
  packed: false
  set:
    key: AXopPNuD0dYw32X/
    flattened: true
    subspace_reference: false
    packed: false
    set: ~
    children:
      inner:
        key: AXopPNuD9Uj5dqm6
        flattened: true
        subspace_reference: false
        packed: false
        set: ~
        children:
          end:
            key: AXopPNuDeX+Xo1Ot
            flattened: false
            subspace_reference: false 
            packed: false
            set: ~
            children: {}
          start:
            key: AXopPNuDkuy4VOO5
            flattened: false
            subspace_reference: false 
            packed: false
            set: ~
            children: {}
      id:
        key: AXopPNuDDI5v9lzf
        flattened: false
        subspace_reference: false
        packed: false
        set: ~
        children: {}
  children: {}
```

The storage plan specifies the paths to all the fields reachable from exports. For example:

```
items -> [AXopPNuD3RsfWb7N]
items[id = "test"] -> [AXopPNuD3RsfWb7N] [<set_key>] [AXopPNuD0dYw32X/]
items[id = "test"].inner -> [AXopPNuD3RsfWb7N] [<set_key>] [AXopPNuD9Uj5dqm6]
items[id = "test"].inner.start -> [AXopPNuD3RsfWb7N] [<set_key>] [AXopPNuDkuy4VOO5]
items[id = "test"].inner.end -> [AXopPNuD3RsfWb7N] [<set_key>] [AXopPNuDeX+Xo1Ot]
items[id = "test"].id -> [AXopPNuD3RsfWb7N] [<set_key>] [AXopPNuDDI5v9lzf]
```

The *set key* is derived from the value of the primary key of the member table type.

Note that fields are *flattened* when doing so does not lead to ambiguity: `.inner.start` and
`.inner.end` have the same key length as `.inner` and `.id`. Currently two kinds of types are
not flattened, for obvious reasons:

- Sets.
- Recursive tables.
