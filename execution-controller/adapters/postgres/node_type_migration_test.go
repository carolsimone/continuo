//go:build integration

package postgres_test

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// v6RewriteQuery extracts the expression and predicate from V6's single
// field-scoped UPDATE. Its complete statement must match before either part
// is evaluated in a read-only SELECT over a JSON fixture.
func v6RewriteQuery(t *testing.T) string {
	t.Helper()
	_, thisFile, _, _ := runtime.Caller(0)
	repoRoot := filepath.Dir(filepath.Dir(filepath.Dir(filepath.Dir(thisFile))))
	b, err := os.ReadFile(filepath.Join(repoRoot, "db", "migration", "execution", "V6__python_node_type_job_params.sql"))
	require.NoError(t, err)
	body := regexp.MustCompile(`(?m)^\s*--[^\n]*`).ReplaceAllString(string(b), "")
	statement := regexp.MustCompile(`(?s)^\s*UPDATE\s+deployments\s+SET\s+job_params\s*=\s*([^;]+?)\s+WHERE\s+([^;]+)\s*;\s*$`)
	parts := statement.FindStringSubmatch(strings.TrimSpace(body))
	require.Len(t, parts, 3, "V6 must contain exactly one deployments job_params UPDATE")
	return `SELECT (CASE WHEN ` + parts[2] + ` THEN ` + parts[1] + ` ELSE job_params END)::text
		FROM (SELECT $1::jsonb AS job_params) AS fixture`
}

// TestV6_RewritesJobParamsNodeTypeOnly evaluates V6's normalization expression
// and predicate without applying the migration. The expression changes only
// node_type, preserves unrelated fields and kinds, and is idempotent.
func TestV6_RewritesJobParamsNodeTypeOnly(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	tx, err := db.BeginTxx(context.Background(), &sql.TxOptions{ReadOnly: true})
	require.NoError(t, err)
	defer func() { require.NoError(t, tx.Rollback()) }()
	query := v6RewriteQuery(t)
	cases := []struct {
		name, input, want string
	}{
		{
			name:  "python node",
			input: `{"node_type":"python-model","service_name":"svc","table_name":"orders"}`,
			want:  `{"node_type":"python-node","service_name":"svc","table_name":"orders"}`,
		},
		{
			name:  "unrelated dbt node",
			input: `{"node_type":"dbt-model","service_name":"svc","table_name":"orders"}`,
			want:  `{"node_type":"dbt-model","service_name":"svc","table_name":"orders"}`,
		},
		{
			name:  "mention in service name",
			input: `{"node_type":"dbt-model","service_name":"python-model","table_name":"orders"}`,
			want:  `{"node_type":"dbt-model","service_name":"python-model","table_name":"orders"}`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var normalized string
			require.NoError(t, tx.Get(&normalized, query, tc.input))
			require.JSONEq(t, tc.want, normalized)
			var twice string
			require.NoError(t, tx.Get(&twice, query, normalized))
			require.JSONEq(t, normalized, twice, "second normalization is a no-op")
		})
	}
}
