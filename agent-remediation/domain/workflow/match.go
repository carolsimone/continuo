package workflow

import "slices"

// Match says which failing clusters a type handles. An empty Source matches
// any source, nil NodeTypes matches any node type, and a failure that carries
// no node type is matched by NodeTypeNone in NodeTypes.
type Match struct {
	Cluster   ClusterKind
	Source    string
	NodeTypes []string
	Pos       Pos
}

// Selects reports whether the match covers a cluster of the given kind whose
// failure comes from source and whose node has nodeType ("" when it has none).
func (m Match) Selects(cluster ClusterKind, source, nodeType string) bool {
	if m.Cluster != cluster {
		return false
	}
	if m.Source != "" && m.Source != source {
		return false
	}
	if m.NodeTypes == nil {
		return true
	}
	if nodeType == "" {
		nodeType = NodeTypeNone
	}
	return slices.Contains(m.NodeTypes, nodeType)
}

// Overlaps reports whether some failure is selected by both matches: the
// clusters are equal, the sources are equal or either is empty, and the node
// type sets intersect or either is nil.
func (m Match) Overlaps(o Match) bool {
	if m.Cluster != o.Cluster {
		return false
	}
	if m.Source != "" && o.Source != "" && m.Source != o.Source {
		return false
	}
	if m.NodeTypes == nil || o.NodeTypes == nil {
		return true
	}
	return slices.ContainsFunc(m.NodeTypes, func(t string) bool {
		return slices.Contains(o.NodeTypes, t)
	})
}
