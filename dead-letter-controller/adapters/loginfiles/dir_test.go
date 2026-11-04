package loginfiles_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/carolsimone/continuo/dead-letter-controller/adapters/loginfiles"
	"github.com/carolsimone/continuo/dead-letter-controller/service/ports"
)

func writeLogin(t *testing.T, dir, username, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, username), []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestDirListsEveryLoginInUsernameOrderTrimmingOneNewline(t *testing.T) {
	dir := t.TempDir()
	writeLogin(t, dir, "continuo_ui", "ui-password\n")
	writeLogin(t, dir, "continuo_catalog", "catalog-password\r\n")
	logins, err := loginfiles.New(dir).Logins(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	want := []ports.RedisLogin{
		{Username: "continuo_catalog", Password: "catalog-password"},
		{Username: "continuo_ui", Password: "ui-password"},
	}
	if len(logins) != len(want) {
		t.Fatalf("logins = %+v, want %+v", logins, want)
	}
	for i := range want {
		if logins[i] != want[i] {
			t.Fatalf("logins[%d] = %+v, want %+v", i, logins[i], want[i])
		}
	}
}

func TestDirReportsAnEmptyPasswordFileOnItsLogin(t *testing.T) {
	dir := t.TempDir()
	writeLogin(t, dir, "continuo_remediation", "\n")
	logins, err := loginfiles.New(dir).Logins(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(logins) != 1 || logins[0].Username != "continuo_remediation" || logins[0].ReadErr == nil {
		t.Fatalf("logins = %+v, want continuo_remediation with a read error", logins)
	}
}

// A Kubernetes Secret mounted as a directory holds each key as a symbolic link
// into a hidden ..data directory: the links are the logins, the hidden entries
// are not.
func TestDirReadsAMountedSecretDirectory(t *testing.T) {
	dir := t.TempDir()
	data := filepath.Join(dir, "..2026_11_04_10_00_00.000000001")
	if err := os.Mkdir(data, 0o700); err != nil {
		t.Fatal(err)
	}
	writeLogin(t, data, "continuo_ui", "ui-password\n")
	if err := os.Symlink(filepath.Base(data), filepath.Join(dir, "..data")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join("..data", "continuo_ui"), filepath.Join(dir, "continuo_ui")); err != nil {
		t.Fatal(err)
	}
	logins, err := loginfiles.New(dir).Logins(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(logins) != 1 || logins[0] != (ports.RedisLogin{Username: "continuo_ui", Password: "ui-password"}) {
		t.Fatalf("logins = %+v, want only continuo_ui", logins)
	}
}

func TestDirFailsOnAnUnreadableDirectory(t *testing.T) {
	_, err := loginfiles.New(filepath.Join(t.TempDir(), "missing")).Logins(context.Background())
	if err == nil || !strings.Contains(err.Error(), "REDIS_LOGINS_DIR") {
		t.Fatalf("err = %v, want it to name REDIS_LOGINS_DIR", err)
	}
}
