package fix_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/carolsimone/continuo/agent-remediation/domain/fix"
)

func TestCleanedReason_ReplacesACredentialAndAnEmailAddress(t *testing.T) {
	got := fix.CleanedReason("dial postgres://u:hunter2@db/x refused, contact ops@acme.com")

	assert.Equal(t, "dial postgres://u:[redacted:secret]@db/x refused, contact [redacted:email]", got)
}

func TestCleanedReason_LeavesATextWithNoSecretAsItIs(t *testing.T) {
	for _, text := range []string{
		"", "release check timed out", "model.sales.orders: column x does not exist",
		`step "dbt_log" failed 5 times, the last time with: s3 is down`,
	} {
		assert.Equal(t, text, fix.CleanedReason(text))
	}
}
