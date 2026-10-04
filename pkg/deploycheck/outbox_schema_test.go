package deploycheck

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Every outbox table must carry the indexes pkg/outbox's queries rely on (the
// claim, the per-aggregate order check, the dead-letter count) and the trigger
// that wakes its relay on commit.
func TestEveryOutboxTableHasClaimIndexesAndNotifyTrigger(t *testing.T) {
	dirs, err := filepath.Glob(filepath.Join(repoRoot(t), "db", "migration", "*"))
	require.NoError(t, err)
	createTable := regexp.MustCompile(`(?i)CREATE TABLE (?:IF NOT EXISTS )?([a-z_]+_outbox)\b`)
	tables := 0
	for _, dir := range dirs {
		files, err := filepath.Glob(filepath.Join(dir, "V*.sql"))
		require.NoError(t, err)
		var all strings.Builder
		for _, f := range files {
			b, err := os.ReadFile(f) //nolint:gosec // G304: f comes from globbing db/migration under repoRoot(t), not external input
			require.NoError(t, err)
			all.Write(b)
			all.WriteString("\n")
		}
		sqlText := strings.Join(strings.Fields(all.String()), " ")
		for _, m := range createTable.FindAllStringSubmatch(sqlText, -1) {
			table := m[1]
			tables++
			for _, want := range []string{
				fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_%[1]s_claimable ON %[1]s (created_at, id) WHERE status IN ('pending', 'scheduled');", table),
				fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_%[1]s_open_by_aggregate ON %[1]s (aggregate_type, aggregate_id, created_at) WHERE status IN ('pending', 'scheduled');", table),
				fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_%[1]s_failed ON %[1]s (created_at) WHERE status = 'failed';", table),
				fmt.Sprintf("CREATE TRIGGER %[1]s_notify AFTER INSERT ON %[1]s FOR EACH STATEMENT EXECUTE FUNCTION continuo_outbox_notify();", table),
			} {
				assert.Contains(t, sqlText, want, "%s/%s", filepath.Base(dir), table)
			}
		}
	}
	assert.Equal(t, 6, tables, "outbox tables found under db/migration")
}
