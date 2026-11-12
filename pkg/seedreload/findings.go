package seedreload

import "strings"

// Findings is what a preflight of a seed's table found in the warehouse.
type Findings struct {
	// DropSemantics is how the engine treats a drop of a table that views read.
	DropSemantics string
	// Completeness says whether the dependants listed are all there are.
	Completeness string
	// Checked is true when the preflight ran to the end.
	Checked bool
	// Dependents are the objects that depend on the seed's table.
	Dependents []Dependent
	// Probes are the answers of the probe of each view the run rebuilds.
	Probes []Probe
}

// Dependent is one object in the warehouse that depends on the seed's table.
type Dependent struct {
	Schema, Name, Kind, Owner string
	// Depth is the number of objects between the seed's table and this one.
	Depth int
	// Allowed is true when the run rebuilds the object or an operator named it.
	Allowed bool
}

// Object returns the dependant's "<schema>.<name>", lowercased.
func (d Dependent) Object() string {
	return strings.ToLower(d.Schema + "." + d.Name)
}

// Probe is the answer of one view's probe after a rebuild.
type Probe struct {
	Schema, Name string
	OK           bool
	Error        string
}

// Outside returns the dependants that are not allowed, in their order.
func (f Findings) Outside() []Dependent {
	var out []Dependent
	for _, d := range f.Dependents {
		if !d.Allowed {
			out = append(out, d)
		}
	}
	return out
}
