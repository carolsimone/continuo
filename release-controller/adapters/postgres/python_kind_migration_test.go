//go:build integration

package postgres_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Release migration V30 refuses to apply while any service is still released
// as kind python. The shared test database has every migration applied and
// holds no such row, so this test builds a private Postgres server from the
// release migration files up to V29 and never touches the shared database.

// pythonKindRefusal is the version of the release migration under test.
const pythonKindRefusal = 30

// TestV30_RefusesWhileAServiceIsStillPython runs V30 against service_prod rows
// of each kind: a service on python stops it, naming every such service sorted
// and comma-separated, and the refused migration changes no row. With no
// python service left it passes, on a table of dbt and python-odcs services and
// on an empty one.
func TestV30_RefusesWhileAServiceIsStillPython(t *testing.T) {
	db := releaseDBBefore(t, pythonKindRefusal)
	v30 := releaseMigrationSQL(t, pythonKindRefusal)

	insert := func(name, kind string) {
		t.Helper()
		mustExec(t, db, `INSERT INTO service_prod (service_name, release_id, manifest_s3_key, image_tag, manifest_kind)
			VALUES ('`+name+`', 'rel-1', 's3://b/`+name+`/manifest.json', 'img:1', '`+kind+`')`)
	}
	rows := func() int {
		t.Helper()
		var n int
		require.NoError(t, db.Get(&n, `SELECT count(*) FROM service_prod`))
		return n
	}

	t.Run("passes on an empty service_prod", func(t *testing.T) {
		_, err := db.Exec(v30)
		require.NoError(t, err)
	})

	insert("svc-dbt", "dbt")
	insert("svc-odcs", "python-odcs")

	t.Run("passes while every service is dbt or python-odcs", func(t *testing.T) {
		_, err := db.Exec(v30)
		require.NoError(t, err)
	})

	t.Run("refuses one service on python and names it", func(t *testing.T) {
		insert("svc-py", "python")
		_, err := db.Exec(v30)
		require.Error(t, err, "the migration must refuse")
		assert.Contains(t, err.Error(), `these services are still released as kind "python": svc-py. `+
			`Release each one as "python-odcs" (docs/contracts.md) on the continuo version you run now, then upgrade.`)
		assert.Equal(t, 3, rows(), "a refused migration changes no row")
	})

	t.Run("names every service on python, sorted and comma-separated", func(t *testing.T) {
		insert("svc-a-py", "python")
		_, err := db.Exec(v30)
		require.Error(t, err, "the migration must refuse")
		assert.Contains(t, err.Error(), `these services are still released as kind "python": svc-a-py, svc-py. Release each one`)
		assert.NotContains(t, err.Error(), "svc-dbt")
		assert.NotContains(t, err.Error(), "svc-odcs")
	})

	t.Run("passes once no service is left on python", func(t *testing.T) {
		mustExec(t, db, `UPDATE service_prod SET manifest_kind = 'python-odcs' WHERE manifest_kind = 'python'`)
		_, err := db.Exec(v30)
		require.NoError(t, err)
	})
}
