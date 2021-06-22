# RefineDB

A strongly-typed schema layer for any transactional key-value database.

Currently supported backends: [FoundationDB](https://github.com/apple/foundationdb/), [TiKV](https://github.com/tikv/tikv). 

*Warning: not ready for production*

## Motivation

I built RefineDB for my personal projects that need a database.

Many applications don't have a complex enough data model to need a fully-featured SQL database, but a key-value database is
difficult to maintain if your data has *some* structure: it has no types or schemas, manually constructing and interpreting
keys is error-prone, and version upgrades require non-trivial handling in application code.

RefineDB allows to keep your database schema in the same repository as your application, type-checks the schema, and handles
version upgrades automatically and safely.

## The type system

In RefineDB, schemas are defined with types:

```
```

The primitive types are:

- `int64`: 64-bit signed integer.
- `double`: IEEE 754 double-precision floating point number.
- `string`: UTF-8 string.
- `bytes`: Byte array.
- `set<T>`: A set with element type `T`.

*Recursive types* are allowed and you can actually construct something like a binary tree:

```
type BinaryTree<T> {
  left: BinaryTree<T>?,
  right: BinaryTree<T>?,
  value: T?,
}
export BinaryTree<int64> data;
```

Note that recursive types are represented using key subspaces, and the performance might be suboptimal if your query path includes
a lot of recursive types (RefineDB only flattens the query path for non-recursive types).

Sum types are nice to have too, but I haven't implemented it yet.

## Queries: the TreeWalker DFG VM

Queries on RefineDB are encoded in a custom bytecode format called the *TreeWalker DFG*.

## Storage plan and schema migration

A storage plan is how a schema maps to entries in the key-value store. By separating schemas and storage plans, RefineDB's
schemas are just "views" of the underlying data structure and schema changes are fast.

During a migration, added fields are automatically assigned new storage keys, and removed fields will not be auto-deleted from
the storage (garbage collection is not yet implemented). This allows multiple schema versions to co-exist and the client can
choose which schema version to use.

## Design docs

[Storage](design/storage.md)

[TreeWalker](design/treewalker.md)

[RefineQL](design/refineql.md)
