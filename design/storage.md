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
    key: AXotdfGWgwakktB+
    flattened: false
    subspace_reference: ~
    packed: false
    set:
      key: AXotdfGWKeLiE7Fz
      flattened: true
      subspace_reference: ~
      packed: false
      set: ~
      children:
        id:
          key: AXotdfGWA87Wm+Ur
          flattened: false
          subspace_reference: ~
          packed: false
          set: ~
          children: {}
        value:
          key: AXotdfGWs5GgKdZm
          flattened: true
          subspace_reference: ~
          packed: false
          set: ~
          children:
            end:
              key: AXotdfGWbEWE3EGj
              flattened: false
              subspace_reference: ~
              packed: false
              set: ~
              children: {}
            start:
              key: AXotdfGWIeQU3Q0Y
              flattened: false
              subspace_reference: ~
              packed: false
              set: ~
              children: {}
    children: {}
  recursive_items:
    key: AXotdfGW1VpbW1gG
    flattened: false
    subspace_reference: ~
    packed: false
    set:
      key: AXotdfGWGyKKcTOs
      flattened: true
      subspace_reference: ~
      packed: false
      set: ~
      children:
        id:
          key: AXotdfGWf7wisfyA
          flattened: false
          subspace_reference: ~
          packed: false
          set: ~
          children: {}
        recursive:
          key: AXotdfGWTYDkb+12
          flattened: false
          subspace_reference: AXotdfGWGyKKcTOs
          packed: false
          set: ~
          children: {}
        value:
          key: AXotdfGWoLaa9Rub
          flattened: true
          subspace_reference: ~
          packed: false
          set: ~
          children:
            end:
              key: AXotdfGWpTkrojrV
              flattened: false
              subspace_reference: ~
              packed: false
              set: ~
              children: {}
            start:
              key: AXotdfGWY2xf+e24
              flattened: false
              subspace_reference: ~
              packed: false
              set: ~
              children: {}
    children: {}
```

The storage plan specifies the paths to all the fields reachable from exports. For example:

```
items -> [AXotdfGWgwakktB+]
items[id == String("hello")] -> [AXotdfGWgwakktB+] [AmhlbGxvAA==] [AXotdfGWKeLiE7Fz]
items[id == String("hello")].id -> [AXotdfGWgwakktB+] [AmhlbGxvAA==] [AXotdfGWA87Wm+Ur]
items[id == String("hello")].value -> [AXotdfGWgwakktB+] [AmhlbGxvAA==] [AXotdfGWs5GgKdZm]
items[id == String("hello")].value.end -> [AXotdfGWgwakktB+] [AmhlbGxvAA==] [AXotdfGWbEWE3EGj]
items[id == String("hello")].value.start -> [AXotdfGWgwakktB+] [AmhlbGxvAA==] [AXotdfGWIeQU3Q0Y]
recursive_items -> [AXotdfGW1VpbW1gG]
recursive_items[id == String("hello")] -> [AXotdfGW1VpbW1gG] [AmhlbGxvAA==] [AXotdfGWGyKKcTOs]
recursive_items[id == String("hello")].id -> [AXotdfGW1VpbW1gG] [AmhlbGxvAA==] [AXotdfGWf7wisfyA]
recursive_items[id == String("hello")].recursive -> [AXotdfGW1VpbW1gG] [AmhlbGxvAA==] [AXotdfGWTYDkb+12]
recursive_items[id == String("hello")].recursive! -> [AXotdfGW1VpbW1gG] [AmhlbGxvAA==] [AXotdfGWTYDkb+12]
recursive_items[id == String("hello")].recursive!.id -> [AXotdfGW1VpbW1gG] [AmhlbGxvAA==] [AXotdfGWTYDkb+12] [AXotdfGWf7wisfyA]
recursive_items[id == String("hello")].recursive!.value -> [AXotdfGW1VpbW1gG] [AmhlbGxvAA==] [AXotdfGWTYDkb+12] [AXotdfGWoLaa9Rub]
recursive_items[id == String("hello")].recursive!.value! -> [AXotdfGW1VpbW1gG] [AmhlbGxvAA==] [AXotdfGWTYDkb+12] [AXotdfGWoLaa9Rub]
recursive_items[id == String("hello")].recursive!.value!.end -> [AXotdfGW1VpbW1gG] [AmhlbGxvAA==] [AXotdfGWTYDkb+12] [AXotdfGWpTkrojrV]
recursive_items[id == String("hello")].recursive!.value!.start -> [AXotdfGW1VpbW1gG] [AmhlbGxvAA==] [AXotdfGWTYDkb+12] [AXotdfGWY2xf+e24]
recursive_items[id == String("hello")].value -> [AXotdfGW1VpbW1gG] [AmhlbGxvAA==] [AXotdfGWoLaa9Rub]
recursive_items[id == String("hello")].value! -> [AXotdfGW1VpbW1gG] [AmhlbGxvAA==] [AXotdfGWoLaa9Rub]
recursive_items[id == String("hello")].value!.end -> [AXotdfGW1VpbW1gG] [AmhlbGxvAA==] [AXotdfGWpTkrojrV]
recursive_items[id == String("hello")].value!.start -> [AXotdfGW1VpbW1gG] [AmhlbGxvAA==] [AXotdfGWY2xf+e24]
```

The *set key* is derived from the value of the primary key of the member table type.

Note that fields are *flattened* when doing so does not lead to ambiguity: `.value.start` and
`.value.end` have the same key length as `.value` and `.id`. Currently two kinds of types are
not flattened, for obvious reasons:

- Sets.
- Recursive tables.
