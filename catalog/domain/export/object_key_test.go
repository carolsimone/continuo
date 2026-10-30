package export

import (
	"errors"
	"testing"
	"time"
)

func TestObjectKey_IsTheTenantsBackupPathAtTheUTCSecond(t *testing.T) {
	at := time.Date(2026, 10, 30, 3, 4, 5, 999_000_000, time.FixedZone("CET", 3600))
	key, err := ObjectKey("default", at)
	if err != nil {
		t.Fatal(err)
	}
	if want := "tenants/default/backups/catalog/20261030T020405Z.jsonl.gz"; key != want {
		t.Fatalf("ObjectKey = %q, want %q", key, want)
	}
	prefix, err := ObjectPrefix("default")
	if err != nil {
		t.Fatal(err)
	}
	if want := "tenants/default/backups/catalog/"; prefix != want {
		t.Fatalf("ObjectPrefix = %q, want %q", prefix, want)
	}
}

func TestObjectKey_RefusesATenantThatCannotNameAPath(t *testing.T) {
	for _, tenantID := range []string{"", "a/b", "/", "../x"} {
		if _, err := ObjectKey(tenantID, time.Now()); !errors.Is(err, ErrNotAnExportKey) {
			t.Errorf("ObjectKey(%q) err = %v, want ErrNotAnExportKey", tenantID, err)
		}
		if _, err := ObjectPrefix(tenantID); !errors.Is(err, ErrNotAnExportKey) {
			t.Errorf("ObjectPrefix(%q) err = %v, want ErrNotAnExportKey", tenantID, err)
		}
	}
}

func TestParseObjectKey_ReturnsTheTenantAndTheTime(t *testing.T) {
	tenantID, at, err := ParseObjectKey("tenants/t-1/backups/catalog/20261030T020405Z.jsonl.gz")
	if err != nil {
		t.Fatal(err)
	}
	if tenantID != "t-1" || !at.Equal(time.Date(2026, 10, 30, 2, 4, 5, 0, time.UTC)) || at.Location() != time.UTC {
		t.Fatalf("ParseObjectKey = %q, %s", tenantID, at)
	}
}

// Only a key ObjectKey writes is an export's key: anything else under or
// beside the backup prefix is refused, so a restore or a retention pass
// never touches another object.
func TestParseObjectKey_RefusesEveryOtherKey(t *testing.T) {
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
		"tenants/a/b/backups/catalog/20261030T020405Z.jsonl.gz",
		"tenants/t/topologies/r1/topology.json.gz",
		"/tenants/t/backups/catalog/20261030T020405Z.jsonl.gz",
		"s3://continuo/tenants/t/backups/catalog/20261030T020405Z.jsonl.gz",
		"code-bundles/r1/bundle.json",
	} {
		if _, _, err := ParseObjectKey(key); !errors.Is(err, ErrNotAnExportKey) {
			t.Errorf("ParseObjectKey(%q) err = %v, want ErrNotAnExportKey", key, err)
		}
	}
}

// Keys sort by the time they name, so the newest export is the greatest key.
func TestObjectKey_SortsByTime(t *testing.T) {
	older, _ := ObjectKey("t", time.Date(2026, 9, 30, 23, 59, 59, 0, time.UTC))
	newer, _ := ObjectKey("t", time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC))
	if older >= newer {
		t.Fatalf("%q sorts after %q", older, newer)
	}
}
