package ports

import "context"

// SchemaColumns reads which columns the state database has.
type SchemaColumns interface {
	// Missing returns, in the order given, the columns of want
	// ("<table>.<column>") the current schema does not have.
	Missing(ctx context.Context, want []string) ([]string, error)
}
