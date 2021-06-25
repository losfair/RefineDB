use anyhow::Result;
use rdb_analyzer::data::treewalker::vm::TwVm;
use serde::Serialize;

#[derive(Serialize)]
pub struct VisNode {
  id: usize,
  label: String,
  shape: Option<String>,
  group: usize,
  x: Option<f64>,
  y: Option<f64>,
  fixed: Option<VisNodeFixed>,
}

#[derive(Serialize)]
struct VisNodeFixed {
  x: bool,
  y: bool,
}

#[derive(Serialize)]
pub struct VisEdge {
  from: usize,
  to: usize,
  dashes: bool,
  color: Option<String>,
  arrows: &'static str,
  label: Option<String>,
}

#[derive(Serialize, Default)]
pub struct VisualizedDataflow {
  nodes: Vec<VisNode>,
  edges: Vec<VisEdge>,
}

struct Visualizer<'a, 'b> {
  vm: &'b TwVm<'a>,
  output: VisualizedDataflow,
}

impl<'a, 'b> Visualizer<'a, 'b> {
  fn visualize_df(&mut self) -> Result<()> {
    for (i, g) in self.vm.script.graphs.iter().enumerate() {
      let id = self.output.nodes.len();
      assert_eq!(id, i);
      self.output.nodes.push(VisNode {
        id,
        label: format!("graph:{}", g.name),
        shape: Some("diamond".into()),
        group: i,
        x: None,
        y: None,
        fixed: None,
      });
    }
    let n_graphs = self.vm.script.graphs.len();
    for i in 0..n_graphs {
      self.visualize_graph(i)?;
    }
    Ok(())
  }

  fn visualize_graph(&mut self, graph_index: usize) -> Result<()> {
    let g = &self.vm.script.graphs[graph_index];
    let mut node_id_in_output: Vec<usize> = Vec::with_capacity(g.nodes.len());
    for (n, in_edges, condition) in &g.nodes {
      let id = self.output.nodes.len();
      self.output.nodes.push(VisNode {
        id,
        label: format!("{:?}", n),
        shape: None,
        group: graph_index,
        x: None,
        y: None,
        fixed: None,
      });
      let mut has_deps = false;
      for (i, in_edge) in in_edges.iter().enumerate() {
        self.output.edges.push(VisEdge {
          from: node_id_in_output[*in_edge as usize],
          to: id,
          dashes: false,
          color: None,
          arrows: "to",
          label: Some(format!("{}", i)),
        });
        has_deps = true;
      }
      if let Some(x) = condition {
        self.output.edges.push(VisEdge {
          from: node_id_in_output[*x as usize],
          to: id,
          dashes: true,
          color: None,
          arrows: "to",
          label: None,
        });
        has_deps = true;
      }
      if !has_deps {
        self.output.edges.push(VisEdge {
          from: graph_index,
          to: id,
          dashes: true,
          color: None,
          arrows: "to",
          label: None,
        });
      }
      let subgraph_references = n.subgraph_references();
      for subgraph_id in subgraph_references {
        self.output.edges.push(VisEdge {
          from: id,
          to: subgraph_id as usize,
          dashes: true,
          color: Some("red".into()),
          arrows: "to",
          label: None,
        });
      }
      node_id_in_output.push(id);
    }

    Ok(())
  }
}

pub fn visualize_df(vm: &TwVm) -> Result<String> {
  let mut vis = Visualizer {
    vm,
    output: VisualizedDataflow::default(),
  };
  vis.visualize_df()?;
  Ok(serde_json::to_string(&vis.output)?)
}
