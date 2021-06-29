# RefineDB

A strongly-typed document database that runs on any transactional key-value store.

Currently supported backends are:

- [FoundationDB](https://github.com/apple/foundationdb) for distributed deployment.
- [SQLite](https://www.sqlite.org/index.html) for single-machine deployment.
- A simple in-memory key-value store for the web playground.

Try RefineDB on the [Web Playground](https://playground.rdb.univalence.me/)!

**Warning: Not ready for production.**

## Motivation

Databases should be more scalable than popular SQL databases, more structured than popular NoSQL databases, and support stronger
static type checking than any of the current databases. So I decided to build RefineDB as "the kind of database that I want to use myself".

RefineDB will be used as the database service of [rusty-workers](https://github.com/losfair/rusty-workers).

## Architecture

![Architecture](https://univalence.me/i/d32378a2042ef32d15bef3dd6dc1b73c_5100183c11cb7b6aa2a8049c00d80ffc.svg)

## Getting started

Examples are a TODO but rdb-analyzer's [tests](https://github.com/losfair/RefineDB/blob/main/rdb-analyzer/src/data/treewalker/asm/asm_test.rs) and `rdb-server` (which uses RefineDB itself to store metadata) should give some basic insight on how the system works.

## Schemas and the type system

In RefineDB, schemas are defined with types. For example, a part of a schema for a simple blog would look like:

```
type SiteConfig {
  site_name: string,
  registration_open: int64,
}

type BlogPost {
  @primary
  id: string,
  author_email: string,
  author_name: string,
  title: string,
  content: string,
  access_time: AccessTime,
}

type AccessTime {
  create_time: int64,
  update_time: int64,
}

export SiteConfig site_config;
export set<BlogPost> posts;
```

The primitive types are:

- `int64`: 64-bit signed integer.
- `double`: IEEE 754 double-precision floating point number.
- `string`: UTF-8 string.
- `bytes`: Byte array.
- `set<T>`: A set with element type `T`.

Sum types are nice to have too, but I haven't implemented it yet.

## Queries: the TreeWalker VM and RefineAsm

Queries in RefineDB are encoded as *data flow graphs*, and query execution is graph reduction.

The TreeWalker VM is a massively concurrent data flow virtual machine for running the queries, but I haven't written documentation
on its internals.

RefineAsm is the textual representation of the query graph, with some syntactic sugar to make writing it easier.

An example RefineAsm script for adding a post to the above blog schema:

```
type PostMap = map {
  id: string,
  author_email: string,
  author_name: string,
  title: string,
  content: string,
  access_time: map {
    create_time: int64,
    update_time: int64,
  },
};
export graph add_post(root: schema, post: PostMap) {
  s_insert root.posts $ call(build_post) [post];
}
graph build_post(post: PostMap): BlogPost {
  return build_table(BlogPost)
    $ m_insert(access_time) (build_table(AccessTime) post.access_time) post;
}
```

## Storage plan and schema migration

A storage plan is how a schema maps to entries in the key-value store. By separating schemas and storage plans, RefineDB's
schemas are just "views" of the underlying keyspace and schema changes are fast.

During a migration, added fields are automatically assigned new storage keys, and removed fields will not be auto-deleted from
the storage. This allows multiple schema versions to co-exist, enables the client to choose which schema version to use, and
prevents unintended data deletion.

[Storage design doc](design/storage.md)

## License

MIT
