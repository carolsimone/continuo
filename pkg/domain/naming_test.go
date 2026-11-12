package domain

import (
	"strconv"
	"strings"
	"testing"
)

func TestComputeJobName(t *testing.T) {
	const runID = "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
	const shortSuffix = "a1b2c3d4" // first 8 chars of runID

	tests := []struct {
		name        string
		service     string
		schema      string
		table       string
		runID       string
		attempt     int
		expected    string
		expectError bool
	}{
		{
			name:     "simple case",
			service:  "analytics",
			schema:   "public",
			table:    "users",
			runID:    runID,
			expected: "analytics-public-users-" + shortSuffix,
		},
		{
			name:     "uppercase converted to lowercase",
			service:  "Analytics",
			schema:   "PUBLIC",
			table:    "Users",
			runID:    runID,
			expected: "analytics-public-users-" + shortSuffix,
		},
		{
			name:     "underscores converted to hyphens",
			service:  "my_service",
			schema:   "prod_schema",
			table:    "user_table",
			runID:    runID,
			expected: "my-service-prod-schema-user-table-" + shortSuffix,
		},
		{
			name:     "dots converted to hyphens",
			service:  "prod.service",
			schema:   "public.schema",
			table:    "user.table",
			runID:    runID,
			expected: "prod-service-public-schema-user-table-" + shortSuffix,
		},
		{
			name:     "special chars sanitized",
			service:  "my@service",
			schema:   "prod#schema",
			table:    "user$table",
			runID:    runID,
			expected: "my-service-prod-schema-user-table-" + shortSuffix,
		},
		{
			name:     "consecutive hyphens collapsed",
			service:  "my--service",
			schema:   "prod___schema",
			table:    "user..table",
			runID:    runID,
			expected: "my-service-prod-schema-user-table-" + shortSuffix,
		},
		{
			name:     "leading and trailing hyphens removed",
			service:  "-service",
			schema:   "_schema_",
			table:    "table-",
			runID:    runID,
			expected: "service-schema-table-" + shortSuffix,
		},
		{
			name:    "max length truncation at 63 chars",
			service: "very-long-service-name-that-exceeds-kubernetes-limits-significantly",
			schema:  "very-long-schema-name",
			table:   "very-long-table-name",
			runID:   runID,
			// The prefix is cut to 47 characters, a trailing hyphen trimmed, and
			// ends with the first six hex characters of the full prefix's
			// SHA-256; the run-id suffix stays whole.
			expected: "very-long-service-name-that-exceeds-kubernetes-ecf83f-a1b2c3d4",
		},
		{
			name:     "all numeric values",
			service:  "123",
			schema:   "456",
			table:    "789",
			runID:    runID,
			expected: "123-456-789-" + shortSuffix,
		},
		{
			name:     "mixed alphanumeric",
			service:  "service1",
			schema:   "schema2",
			table:    "table3",
			runID:    runID,
			expected: "service1-schema2-table3-" + shortSuffix,
		},
		{
			name:    "empty service/schema/table but valid runID suffix",
			service: "___",
			schema:  "...",
			table:   "!!!",
			runID:   runID,
			// Base sanitizes to empty but suffix "a1b2c3d4" produces a valid name.
			expected: shortSuffix,
		},
		{
			name:     "only special characters but valid runID suffix",
			service:  "@#$",
			schema:   "%^&",
			table:    "*()",
			runID:    runID,
			expected: shortSuffix,
		},
		{
			name:     "unicode characters converted",
			service:  "sėrvicė",
			schema:   "schēma",
			table:    "tåble",
			runID:    runID,
			expected: "s-rvic-sch-ma-t-ble-" + shortSuffix,
		},
		{
			name:     "already compliant name",
			service:  "query-executor",
			schema:   "public",
			table:    "metrics-v2",
			runID:    runID,
			expected: "query-executor-public-metrics-v2-" + shortSuffix,
		},
		{
			name:     "runID longer than 8 chars uses only first 8",
			service:  "svc",
			schema:   "sc",
			table:    "t",
			runID:    "abcdef12-longer-uuid-string",
			expected: "svc-sc-t-abcdef12",
		},
		{
			name:     "runID exactly 8 chars",
			service:  "svc",
			schema:   "sc",
			table:    "t",
			runID:    "abcdef12",
			expected: "svc-sc-t-abcdef12",
		},
		{
			// The suffix survives the cut of a long identifier: the prefix
			// sanitizes to 65 characters, keeps 47 with a hash of the full
			// prefix, and the 8-character run-id suffix keeps runs distinct.
			name:     "suffix preserved when prefix exceeds 54 chars",
			service:  "very-long-service-name",
			schema:   "very-long-schema-name",
			table:    "very-long-table-name",
			runID:    "a1b2c3d4-xxxx",
			expected: "very-long-service-name-very-long-schema-name-ve-bc2309-a1b2c3d4",
		},
		{
			// Regression: two runs with the same long prefix must NOT collide.
			name:     "different schedule IDs produce different names for long prefix",
			service:  "very-long-service-name",
			schema:   "very-long-schema-name",
			table:    "very-long-table-name",
			runID:    "b5c6d7e8-yyyy",
			expected: "very-long-service-name-very-long-schema-name-ve-bc2309-b5c6d7e8",
		},
		{
			name:     "attempt 1 appends -r1",
			service:  "analytics",
			schema:   "public",
			table:    "users",
			runID:    runID,
			attempt:  1,
			expected: "analytics-public-users-" + shortSuffix + "-r1",
		},
		{
			// The prefix gives up room for "-r1" and ends with the first six hex
			// characters of the full prefix's SHA-256; the run-id suffix stays
			// whole and the name is exactly 63 characters.
			name:     "attempt 1 at the length limit keeps the run-id suffix and tags the cut prefix",
			service:  "very-long-service-name",
			schema:   "very-long-schema-name",
			table:    "very-long-table-name",
			runID:    "a1b2c3d4-xxxx",
			attempt:  1,
			expected: "very-long-service-name-very-long-schema-name-bc2309-a1b2c3d4-r1",
		},
		{
			// A cut that ends on a hyphen is trimmed, so no name holds "--".
			name:     "attempt 10 at the length limit trims the hyphen the cut leaves",
			service:  "very-long-service-name",
			schema:   "very-long-schema-na",
			table:    "very-long-table-name",
			runID:    "a1b2c3d4-xxxx",
			attempt:  10,
			expected: "very-long-service-name-very-long-schema-na-4fc59f-a1b2c3d4-r10",
		},
		{
			name:        "negative attempt is refused",
			service:     "svc",
			schema:      "sc",
			table:       "t",
			runID:       runID,
			attempt:     -1,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ComputeJobName(tt.service, tt.schema, tt.table, tt.runID, tt.attempt)

			if tt.expectError {
				if err == nil {
					t.Errorf("expected error but got none, result: %s", result)
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
					return
				}

				if result != tt.expected {
					t.Errorf("got %q, want %q", result, tt.expected)
				}

				// Validate k8s constraints
				if len(result) > 63 {
					t.Errorf("result length %d exceeds 63 character limit", len(result))
				}

				if len(result) == 0 {
					t.Error("result is empty")
				}

				// Check starts and ends with alphanumeric
				if result[0] == '-' || result[len(result)-1] == '-' {
					t.Errorf("result %q starts or ends with hyphen", result)
				}

				// Check only contains allowed characters
				for _, ch := range result {
					if (ch < 'a' || ch > 'z') && (ch < '0' || ch > '9') && ch != '-' {
						t.Errorf("result %q contains invalid character: %c", result, ch)
					}
				}
			}
		})
	}
}

// Two nodes whose prefixes the cut would make equal still get distinct names
// at every attempt, since a cut prefix carries a hash of the full prefix; the
// run-id suffix stays whole.
func TestComputeJobName_RetriesOfNodesThatDifferOnlyPastTheCutStayDistinct(t *testing.T) {
	const runID = "3f2a9c1e-0000-4000-8000-000000000001"
	daily, weekly := "int_customer_orders_aggregated_daily", "int_customer_orders_aggregated_weekly"
	for _, attempt := range []int{0, 1, 2, 10} {
		a, err := ComputeJobName("service-1", "analytics", daily, runID, attempt)
		if err != nil {
			t.Fatal(err)
		}
		b, err := ComputeJobName("service-1", "analytics", weekly, runID, attempt)
		if err != nil {
			t.Fatal(err)
		}
		if a == b {
			t.Fatalf("attempt %d: both nodes are named %q", attempt, a)
		}
		again, _ := ComputeJobName("service-1", "analytics", daily, runID, attempt)
		if again != a {
			t.Fatalf("attempt %d: %q then %q for the same node", attempt, a, again)
		}
		suffix := "-3f2a9c1e"
		if attempt > 0 {
			suffix += "-r" + strconv.Itoa(attempt)
		}
		for _, name := range []string{a, b} {
			if len(name) > 63 || !strings.HasSuffix(name, suffix) {
				t.Fatalf("attempt %d: %q is longer than 63 or does not end with %q", attempt, name, suffix)
			}
		}
	}
	first, _ := ComputeJobName("service-1", "analytics", daily, runID, 0)
	if first != "service-1-analytics-int-customer-orders-aggrega-3c1270-3f2a9c1e" {
		t.Fatalf("attempt 0 is %q", first)
	}
}

// Two versions of one dbt model differ only at the end of their table name
// ("<name>.v1", "<name>.v2"), where a long prefix is cut, so a cut prefix
// carries a hash of the full prefix at attempt 0 as well: both versions can
// run in one run under distinct Job names.
func TestComputeJobName_VersionsOfALongModelStayDistinctAtAttemptZero(t *testing.T) {
	const runID = "3f2a9c1e-0000-4000-8000-000000000001"
	v1, err := ComputeJobName("orders-service", "analytics_marts", "customer_lifetime_value.v1", runID, 0)
	if err != nil {
		t.Fatal(err)
	}
	v2, err := ComputeJobName("orders-service", "analytics_marts", "customer_lifetime_value.v2", runID, 0)
	if err != nil {
		t.Fatal(err)
	}
	if v1 == v2 {
		t.Fatalf("both versions are named %q", v1)
	}
	for _, name := range []string{v1, v2} {
		if len(name) > 63 || !strings.HasSuffix(name, "-3f2a9c1e") {
			t.Fatalf("%q is longer than 63 or does not end with the run id", name)
		}
	}
}

// A name whose prefix fits is not cut and carries no hash.
func TestComputeJobName_AShortVersionedNameIsUnchanged(t *testing.T) {
	name, err := ComputeJobName("service-1", "analytics", "dim_v.v2", "3f2a9c1e-0000", 0)
	if err != nil {
		t.Fatal(err)
	}
	if name != "service-1-analytics-dim-v-v2-3f2a9c1e" {
		t.Fatalf("got %q", name)
	}
}
