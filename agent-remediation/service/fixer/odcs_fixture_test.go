package fixer

import (
	"fmt"
	"strings"
)

// odcsNode describes one python node's ODCS contract for a test fixture; an
// empty field writes nothing.
type odcsNode struct {
	ID          string
	Kind        string
	Owner       string
	Schedule    string
	Criticality string
	Script      string
	SecretRef   string
	CSVSource   string
	// Reads are the readQueries, name then SQL, in file order.
	Reads [][2]string
	// Columns are the names of the schema object's properties.
	Columns []string
}

// yaml renders the contract as a team writes it: one contract per file, its
// table the part of the id after the last dot.
func (n odcsNode) yaml() string {
	table := n.ID[strings.LastIndex(n.ID, ".")+1:]
	var b strings.Builder
	fmt.Fprintf(&b, "apiVersion: v3.2.0\nkind: DataContract\nid: %s\nname: %s\nversion: \"0\"\n", n.ID, table)
	if n.Owner != "" {
		fmt.Fprintf(&b, "team:\n  name: %s\n", n.Owner)
	}
	b.WriteString("customProperties:\n")
	for _, p := range [][2]string{
		{"kind", n.Kind}, {"schedule", n.Schedule}, {"criticality", n.Criticality},
		{"script", n.Script}, {"secretRef", n.SecretRef}, {"csvSource", n.CSVSource},
	} {
		if p[1] != "" {
			fmt.Fprintf(&b, "  - {vendor: continuo, property: %s, value: %q}\n", p[0], p[1])
		}
	}
	if len(n.Reads) > 0 {
		b.WriteString("  - vendor: continuo\n    property: readQueries\n    value:\n")
		for _, r := range n.Reads {
			fmt.Fprintf(&b, "      %s: %s\n", r[0], r[1])
		}
	}
	fmt.Fprintf(&b, "schema:\n  - name: %s\n    physicalType: table\n    properties:\n", table)
	for _, c := range n.Columns {
		fmt.Fprintf(&b, "      - {name: %s, physicalType: INTEGER}\n", c)
	}
	return b.String()
}

// notAContract is a file the answer empties of its contract: valid yaml that
// declares no node.
const notAContract = "apiVersion: v3.2.0\nkind: DataContract\n"

// withReads renders n with its readQueries replaced by reads.
func withReads(n odcsNode, reads ...[2]string) string {
	n.Reads = reads
	return n.yaml()
}
