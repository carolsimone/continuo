//go:build integration

package postgres

import (
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
)

// The status sets the runner's tables repeat in SQL must equal the Go state
// types' lists, so a status added in one place and not the other fails here.
func TestRemediationStatusChecksMatchTheStateTypes(t *testing.T) {
	db := newTestDB(t)

	var tables int
	require.NoError(t, db.Get(&tables, `SELECT count(*) FROM pg_class
		WHERE relname IN ('remediation','remediation_try','remediation_cluster','remediation_step')
		AND relkind = 'r'`))
	require.Equal(t, 4, tables, "run the migrations (make test-deps-up): the remediation runner tables are missing")

	quoted := regexp.MustCompile(`'([a-z_]+)'`)
	checkValues := func(table string) []string {
		var def string
		require.NoError(t, db.Get(&def, `SELECT pg_get_constraintdef(oid) FROM pg_constraint
			WHERE conrelid = $1::regclass AND contype = 'c' AND pg_get_constraintdef(oid) LIKE '%status%'`, table),
			"%s has one status CHECK", table)
		var out []string
		for _, m := range quoted.FindAllStringSubmatch(def, -1) {
			out = append(out, m[1])
		}
		return out
	}

	var remediationStatuses, tryStatuses, clusterStatuses, stepStatuses []string
	for _, s := range remediation.AllRemediationStatuses() {
		remediationStatuses = append(remediationStatuses, string(s))
	}
	for _, s := range remediation.AllTryStatuses() {
		tryStatuses = append(tryStatuses, string(s))
	}
	for _, s := range remediation.AllClusterStatuses() {
		clusterStatuses = append(clusterStatuses, string(s))
	}
	for _, s := range remediation.AllStepStatuses() {
		stepStatuses = append(stepStatuses, string(s))
	}

	require.ElementsMatch(t, remediationStatuses, checkValues("remediation"), "remediation.status lists exactly remediation.AllRemediationStatuses()")
	require.ElementsMatch(t, tryStatuses, checkValues("remediation_try"), "remediation_try.status lists exactly remediation.AllTryStatuses()")
	require.ElementsMatch(t, clusterStatuses, checkValues("remediation_cluster"), "remediation_cluster.status lists exactly remediation.AllClusterStatuses()")
	require.ElementsMatch(t, stepStatuses, checkValues("remediation_step"), "remediation_step.status lists exactly remediation.AllStepStatuses()")
}
