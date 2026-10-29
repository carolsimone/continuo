package postgres_test

import (
	"regexp"
	"testing"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var quotedLiteral = regexp.MustCompile(`'([^']*)'`)

// TestOperationSchema_TheCheckListsMatchTheVocabulary: the operation CHECK on
// runs and on tasks, read from pg_constraint, lists exactly the values of the
// contract's operation vocabulary. A value added to the contract fails here
// until a migration widens both constraints.
func TestOperationSchema_TheCheckListsMatchTheVocabulary(t *testing.T) {
	db := newTestDB(t)
	want := model.VocabularyValuesForTest()["Operation"]
	require.NotEmpty(t, want, "the contract declares no operation vocabulary")

	for _, c := range []struct{ table, constraint string }{
		{"runs", "runs_operation_check"},
		{"tasks", "tasks_operation_check"},
	} {
		t.Run(c.table, func(t *testing.T) {
			var def string
			require.NoError(t, db.Get(&def, `
				SELECT pg_get_constraintdef(oid)
				FROM pg_constraint
				WHERE conrelid = $1::regclass AND conname = $2 AND contype = 'c'`,
				c.table, c.constraint), "the %s CHECK constraint must exist", c.constraint)

			var got []string
			for _, m := range quotedLiteral.FindAllStringSubmatch(def, -1) {
				got = append(got, m[1])
			}
			assert.ElementsMatch(t, want, got, "%s: %s", c.constraint, def)
		})
	}
}
