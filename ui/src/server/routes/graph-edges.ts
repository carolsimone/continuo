interface WireEdge {
  from_node_id: string;
  to_node_id: string;
}

// executionOrderEdges turns the edges state sends into the edges the client
// reads. State sends each edge in dependency direction: `from` is the
// dependent node, `to` is the node it reads. Every client view (dependency
// depth, swimlane columns, arrowheads, the selected node's upstream/downstream
// sets, the topology layout) reads an edge as "`from` runs before `to`", so the
// edge is reversed exactly once, at the boundary, into execution order.
export function executionOrderEdges(edges: WireEdge[] | undefined): WireEdge[] {
  return (edges || []).map((e) => ({
    from_node_id: e.to_node_id,
    to_node_id: e.from_node_id,
  }));
}
