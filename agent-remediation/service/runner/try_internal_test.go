package runner

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
)

func TestFindingsReason_NamesEveryFindingOfTheCheck(t *testing.T) {
	one := []remediation.Finding{{Code: "path_edited_twice", Message: "two edits change the same file: a.sql", File: "a.sql"}}
	assert.Equal(t, "two edits change the same file: a.sql", findingsReason(one))

	two := []remediation.Finding{one[0], {Code: "path_edited_twice", Message: "two edits change the same file: b.sql"}}
	assert.Equal(t, "two edits change the same file: a.sql; two edits change the same file: b.sql", findingsReason(two))
}
