package s3

import (
	"errors"
	"testing"
	"time"

	"github.com/carolsimone/continuo/catalog/service/ports"
)

func TestBackupKey_IsTheTenantsBackupPathAtTheUTCSecond(t *testing.T) {
	at := time.Date(2026, 10, 30, 3, 4, 5, 999_000_000, time.FixedZone("CET", 3600))
	key, err := backupKey("default", at)
	if err != nil {
		t.Fatal(err)
	}
	if want := "tenants/default/backups/catalog/20261030T020405Z.jsonl.gz"; key != want {
		t.Fatalf("backupKey = %q, want %q", key, want)
	}
	prefix, err := backupPrefix("default")
	if err != nil {
		t.Fatal(err)
	}
	if want := "tenants/default/backups/catalog/"; prefix != want {
		t.Fatalf("backupPrefix = %q, want %q", prefix, want)
	}
}

// A tenant id that would name no directory, or another one, names no prefix:
// "" and "/" name none, "." the backups of no tenant, ".." and anything with
// a slash a path outside the tenant's.
func TestBackupKey_RefusesATenantThatCannotNameAPath(t *testing.T) {
	for _, tenantID := range []string{"", ".", "..", "a/b", "/", "../x"} {
		if _, err := backupKey(tenantID, time.Now()); !errors.Is(err, ports.ErrInvalidTenant) {
			t.Errorf("backupKey(%q) err = %v, want ErrInvalidTenant", tenantID, err)
		}
		if _, err := backupPrefix(tenantID); !errors.Is(err, ports.ErrInvalidTenant) {
			t.Errorf("backupPrefix(%q) err = %v, want ErrInvalidTenant", tenantID, err)
		}
		if err := (BackupKeys{}).CheckTenant(tenantID); !errors.Is(err, ports.ErrInvalidTenant) {
			t.Errorf("CheckTenant(%q) err = %v, want ErrInvalidTenant", tenantID, err)
		}
	}
	if err := (BackupKeys{}).CheckTenant("t-1.prod"); err != nil {
		t.Errorf("CheckTenant(%q) err = %v, want nil", "t-1.prod", err)
	}
}

func TestParseBackupKey_ReturnsTheTenantAndTheTime(t *testing.T) {
	tenantID, at, err := BackupKeys{}.Locate("tenants/t-1/backups/catalog/20261030T020405Z.jsonl.gz")
	if err != nil {
		t.Fatal(err)
	}
	if tenantID != "t-1" || !at.Equal(time.Date(2026, 10, 30, 2, 4, 5, 0, time.UTC)) || at.Location() != time.UTC {
		t.Fatalf("Locate = %q, %s", tenantID, at)
	}
}

// Only a key backupKey writes is a backup's key: anything else under or
// beside the backup prefix is refused, so a restore or a retention pass
// never touches another object.
func TestParseBackupKey_RefusesEveryOtherKey(t *testing.T) {
	for _, key := range []string{
		"",
		"tenants/t/backups/catalog/",
		"tenants/t/backups/catalog/20261030T020405Z.jsonl",
		"tenants/t/backups/catalog/20261030T020405Z.jsonl.gz.tmp",
		"tenants/t/backups/catalog/20261030T0204Z.jsonl.gz",
		"tenants/t/backups/catalog/20261330T020405Z.jsonl.gz",
		"tenants/t/backups/catalog/2026-10-30T02:04:05Z.jsonl.gz",
		"tenants/t/backups/catalog/x/20261030T020405Z.jsonl.gz",
		"tenants//backups/catalog/20261030T020405Z.jsonl.gz",
		"tenants/./backups/catalog/20261030T020405Z.jsonl.gz",
		"tenants/../backups/catalog/20261030T020405Z.jsonl.gz",
		"tenants/a/b/backups/catalog/20261030T020405Z.jsonl.gz",
		"tenants/t/topologies/r1/topology.json.gz",
		"/tenants/t/backups/catalog/20261030T020405Z.jsonl.gz",
		"s3://continuo/tenants/t/backups/catalog/20261030T020405Z.jsonl.gz",
		"code-bundles/r1/bundle.json",
	} {
		if _, _, err := (BackupKeys{}).Locate(key); !errors.Is(err, ports.ErrNotABackupKey) {
			t.Errorf("Locate(%q) err = %v, want ErrNotABackupKey", key, err)
		}
	}
}

// Keys sort by the time they name, so the newest backup is the greatest key.
func TestBackupKey_SortsByTime(t *testing.T) {
	older, _ := backupKey("t", time.Date(2026, 9, 30, 23, 59, 59, 0, time.UTC))
	newer, _ := backupKey("t", time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC))
	if older >= newer {
		t.Fatalf("%q sorts after %q", older, newer)
	}
}
