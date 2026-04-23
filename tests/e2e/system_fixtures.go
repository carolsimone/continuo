package e2e

// getDiamondDAG returns the 13-node diamond DAG (3 seeds + 10 models) used by the happy-path test.
func getDiamondDAG() []dagNode {
	return []dagNode{
		{Name: "seed_table_1", Dependencies: nil, ServiceName: "service-1", NodeType: "dbt-seed"},
		{Name: "seed_table_2", Dependencies: nil, ServiceName: "service-1", NodeType: "dbt-seed"},
		{Name: "seed_table_3", Dependencies: nil, ServiceName: "service-1", NodeType: "dbt-seed"},
		{Name: "table_a", Dependencies: []string{"seed_table_1"}, ServiceName: "service-1"},
		{Name: "table_b", Dependencies: []string{"seed_table_2"}, ServiceName: "service-1"},
		{Name: "table_c", Dependencies: []string{"seed_table_3"}, ServiceName: "service-1"},
		{Name: "table_d", Dependencies: []string{"table_a", "table_b"}, ServiceName: "service-3"},
		{Name: "table_e", Dependencies: []string{"table_b", "table_c"}, ServiceName: "service-3"},
		{Name: "table_f", Dependencies: []string{"table_a", "table_c"}, ServiceName: "service-3"},
		{Name: "table_g", Dependencies: []string{"table_d", "table_e"}, ServiceName: "service-2"},
		{Name: "table_h", Dependencies: []string{"table_e", "table_f"}, ServiceName: "service-2"},
		{Name: "table_i", Dependencies: []string{"table_g", "table_h"}, ServiceName: "service-3"},
		{Name: "table_j", Dependencies: []string{"table_g", "table_h"}, ServiceName: "service-3"},
	}
}

// getDAGLevels returns happy-path DAG nodes grouped by execution level.
func getDAGLevels() [][]string {
	return [][]string{
		{"seed_table_1", "seed_table_2", "seed_table_3"},
		{"table_a", "table_b", "table_c"},
		{"table_d", "table_e", "table_f"},
		{"table_g", "table_h"},
		{"table_i", "table_j"},
	}
}
