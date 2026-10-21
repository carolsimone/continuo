package db

import (
	"context"
	"fmt"

	"github.com/lib/pq"
)

// Selector runs a query and scans its rows into dest. Both *sqlx.DB and a
// transaction-bound executor satisfy it.
type Selector interface {
	SelectContext(ctx context.Context, dest any, query string, args ...any) error
}

// MissingColumns returns the columns of want ("<table>.<column>") the
// connection's current schema does not have, in the order given.
func MissingColumns(ctx context.Context, s Selector, want []string) ([]string, error) {
	missing := []string{}
	if err := s.SelectContext(ctx, &missing, `
		SELECT w.col
		  FROM unnest($1::text[]) WITH ORDINALITY AS w(col, ord)
		 WHERE NOT EXISTS (
		       SELECT 1
		         FROM information_schema.columns c
		        WHERE c.table_schema = current_schema()
		          AND c.table_name::text || '.' || c.column_name::text = w.col)
		 ORDER BY w.ord`, pq.Array(want)); err != nil {
		return nil, fmt.Errorf("read schema columns: %w", err)
	}
	return missing, nil
}
