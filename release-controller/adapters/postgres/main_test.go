//go:build integration

package postgres_test

import (
	"os"
	"testing"

	"github.com/carolsimone/continuo/release-controller/internal/dbtest"
)

// TestMain serialises this binary against the other package that shares the
// continuo_release database.
func TestMain(m *testing.M) {
	os.Exit(dbtest.RunSerialized(m))
}
