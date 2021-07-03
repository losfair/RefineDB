use std::{fmt::Debug, net::ToSocketAddrs, sync::Arc};

use anyhow::Result;
use bumpalo::Bump;
use bytes::Bytes;
use rdb_analyzer::{
  data::treewalker::serialize::{SerializedVmValue, VmValueEncodeConfig},
  schema::{compile::compile, grammar::parse},
  storage_plan::StoragePlan,
};
use warp::{
  hyper::{Body, Response},
  reject::Reject,
  reply::Json,
  Filter, Rejection,
};

use crate::{
  exec_core::{ExecContext, SchemaContext},
  query_cache::QueryCacheKey,
  state::get_state,
  sysquery::{lookup_deployment, lookup_query_script, ns_to_kv_prefix_with_appended_zero},
};

struct ApiReject(anyhow::Error);

impl ApiReject {
  fn new(x: anyhow::Error) -> Self {
    log::error!("api reject: {:?}", x);
    Self(x)
  }
}

impl Debug for ApiReject {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "{}", self.0)
  }
}

impl Reject for ApiReject {}

pub async fn run_http_server(addr: impl ToSocketAddrs) -> ! {
  let query_route_json = warp::path("query")
    .and(warp::path::param()) // namespace
    .and(warp::path::param()) // query script id
    .and(warp::path::param()) // name of the graph
    .and(warp::filters::header::exact(
      "Content-Type",
      "application/json",
    ))
    .and(warp::body::content_length_limit(1024 * 256))
    .and(warp::body::json())
    .and_then(invoke_query);
  let query_route_msgpack = warp::path("query")
    .and(warp::path::param()) // namespace
    .and(warp::path::param()) // query script id
    .and(warp::path::param()) // name of the graph
    .and(warp::filters::header::exact(
      "Content-Type",
      "application/x-msgpack",
    ))
    .and(warp::body::content_length_limit(1024 * 256))
    .and(warp::body::bytes())
    .and_then(invoke_query_msgpack);
  let routes = warp::post().and(query_route_json.or(query_route_msgpack));
  let addr = addr
    .to_socket_addrs()
    .unwrap()
    .next()
    .expect("no socket addrs");
  warp::serve(routes).run(addr).await;
  unreachable!()
}

async fn invoke_query(
  namespace_id: String,
  query_script_id: String,
  graph_name: String,
  graph_params: Vec<SerializedVmValue>,
) -> Result<Json, Rejection> {
  do_invoke_query(
    namespace_id,
    query_script_id,
    graph_name,
    graph_params,
    &Default::default(),
  )
  .await
  .map(|x| warp::reply::json(&x))
  .map_err(|e| warp::reject::custom(ApiReject::new(e)))
}

async fn invoke_query_msgpack(
  namespace_id: String,
  query_script_id: String,
  graph_name: String,
  graph_params: Bytes,
) -> Result<Response<Body>, Rejection> {
  let graph_params: Vec<SerializedVmValue> = rmp_serde::from_slice(&graph_params)
    .map_err(|e| warp::reject::custom(ApiReject::new(anyhow::Error::from(e))))?;
  do_invoke_query(
    namespace_id,
    query_script_id,
    graph_name,
    graph_params,
    &VmValueEncodeConfig {
      enable_bytes: true,
      enable_double: true,
      enable_int64: true,
    },
  )
  .await
  .and_then(|x| rmp_serde::to_vec_named(&x).map_err(anyhow::Error::from))
  .and_then(|x| {
    Response::builder()
      .header("Content-Type", "application/x-msgpack")
      .body(Body::from(x))
      .map_err(anyhow::Error::from)
  })
  .map_err(|e| warp::reject::custom(ApiReject::new(e)))
}

async fn do_invoke_query(
  namespace_id: String,
  query_script_id: String,
  graph_name: String,
  graph_params: Vec<SerializedVmValue>,
  serialization_config: &VmValueEncodeConfig,
) -> Result<SerializedVmValue> {
  let st = get_state();
  let kv_prefix = ns_to_kv_prefix_with_appended_zero(&namespace_id).await?;
  let kv = (st.data_store_generator)(&kv_prefix);

  let exec_ctx;
  if let Some(x) = st
    .query_cache
    .get_hot(&namespace_id, &query_script_id)
    .await
  {
    exec_ctx = x;
  } else {
    let query_script = lookup_query_script(&namespace_id, &query_script_id).await?;

    let qc_key = QueryCacheKey {
      namespace_id: namespace_id.clone(),
      query_script_id: query_script_id.clone(),
      deployment_id: query_script.associated_deployment.clone(),
      query_script_create_time: query_script.create_time,
    };
    if let Some(x) = st.query_cache.get(&qc_key).await {
      exec_ctx = x;
    } else {
      let deployment =
        lookup_deployment(&namespace_id, &query_script.associated_deployment).await?;
      let schema = compile(&parse(&Bump::new(), &deployment.schema)?)?;
      let plan = StoragePlan::deserialize_compressed(&deployment.plan)?;
      let schema_ctx = Arc::new(SchemaContext { schema, plan });
      exec_ctx = Arc::new(ExecContext::load(schema_ctx, &query_script.script)?);
      log::info!("Loaded query script {:?}.", qc_key);
      st.query_cache.put(qc_key, exec_ctx.clone()).await;
    }
  }

  let output = exec_ctx
    .run_exported_graph(&*kv, &graph_name, &graph_params, serialization_config)
    .await?;
  Ok(output)
}
