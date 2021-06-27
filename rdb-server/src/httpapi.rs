use std::{fmt::Debug, net::ToSocketAddrs, sync::Arc};

use anyhow::Result;
use bumpalo::Bump;
use rdb_analyzer::{
  data::treewalker::serialize::SerializedVmValue,
  schema::{compile::compile, grammar::parse},
  storage_plan::StoragePlan,
};
use warp::{reject::Reject, reply::Json, Filter, Rejection};

use crate::{
  exec_core::{ExecContext, SchemaContext},
  query_cache::QueryCacheKey,
  state::get_state,
  sysquery::{lookup_deployment, lookup_query_script, ns_to_kv_prefix_with_appended_zero},
};

struct ApiReject(anyhow::Error);

impl Debug for ApiReject {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "{}", self.0)
  }
}

impl Reject for ApiReject {}

pub async fn run_http_server(addr: impl ToSocketAddrs) -> ! {
  let query_route = warp::path("query")
    .and(warp::path::param()) // namespace
    .and(warp::path::param()) // query script id
    .and(warp::path::param()) // name of the graph
    .and(warp::body::content_length_limit(1024 * 256))
    .and(warp::body::json())
    .and_then(invoke_query);
  let routes = warp::post().and(query_route);
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
  do_invoke_query(namespace_id, query_script_id, graph_name, graph_params)
    .await
    .map_err(|e| warp::reject::custom(ApiReject(e)))
}

async fn do_invoke_query(
  namespace_id: String,
  query_script_id: String,
  graph_name: String,
  graph_params: Vec<SerializedVmValue>,
) -> Result<Json> {
  let st = get_state();
  let kv_prefix = ns_to_kv_prefix_with_appended_zero(&namespace_id).await?;
  let kv = (st.data_store_generator)(&kv_prefix);
  let query_script = lookup_query_script(&namespace_id, &query_script_id).await?;

  let qc_key = QueryCacheKey {
    namespace_id: namespace_id.clone(),
    query_script_id: query_script_id,
    deployment_id: query_script.associated_deployment.clone(),
    query_script_create_time: query_script.create_time,
  };

  let exec_ctx;
  if let Some(x) = st.query_cache.get(&qc_key).await {
    exec_ctx = x;
  } else {
    let deployment = lookup_deployment(&namespace_id, &query_script.associated_deployment).await?;
    let schema = compile(&parse(&Bump::new(), &deployment.schema)?)?;
    let plan = StoragePlan::deserialize_compressed(&deployment.plan)?;
    let schema_ctx = Arc::new(SchemaContext { schema, plan });
    exec_ctx = Arc::new(ExecContext::load(schema_ctx, &query_script.script)?);
    st.query_cache.put(qc_key, exec_ctx.clone()).await;
  }

  let output = exec_ctx
    .run_exported_graph(&*kv, &graph_name, &graph_params)
    .await?;
  Ok(warp::reply::json(&output))
}
