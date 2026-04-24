package handlers

import (
	"testing"

	pkgDomain "github.com/carolsimone/continuo/pkg/domain"
)

func TestComputeJobName(t *testing.T) {
	const scheduleID = "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
	const shortSuffix = "a1b2c3d4" // first 8 chars of scheduleID

	tests := []struct {
		name        string
		service     string
		schema      string
		table       string
		scheduleID  string
		expected    string
		expectError bool
	}{
		{
			name:       "simple case",
			service:    "analytics",
			schema:     "public",
			table:      "users",
			scheduleID: scheduleID,
			expected:   "analytics-public-users-" + shortSuffix,
		},
		{
			name:       "uppercase converted to lowercase",
			service:    "Analytics",
			schema:     "PUBLIC",
			table:      "Users",
			scheduleID: scheduleID,
			expected:   "analytics-public-users-" + shortSuffix,
		},
		{
			name:       "underscores converted to hyphens",
			service:    "my_service",
			schema:     "prod_schema",
			table:      "user_table",
			scheduleID: scheduleID,
			expected:   "my-service-prod-schema-user-table-" + shortSuffix,
		},
		{
			name:       "dots converted to hyphens",
			service:    "prod.service",
			schema:     "public.schema",
			table:      "user.table",
			scheduleID: scheduleID,
			expected:   "prod-service-public-schema-user-table-" + shortSuffix,
		},
		{
			name:       "special chars sanitized",
			service:    "my@service",
			schema:     "prod#schema",
			table:      "user$table",
			scheduleID: scheduleID,
			expected:   "my-service-prod-schema-user-table-" + shortSuffix,
		},
		{
			name:       "consecutive hyphens collapsed",
			service:    "my--service",
			schema:     "prod___schema",
			table:      "user..table",
			scheduleID: scheduleID,
			expected:   "my-service-prod-schema-user-table-" + shortSuffix,
		},
		{
			name:       "leading and trailing hyphens removed",
			service:    "-service",
			schema:     "_schema_",
			table:      "table-",
			scheduleID: scheduleID,
			expected:   "service-schema-table-" + shortSuffix,
		},
		{
			name:       "max length truncation at 63 chars",
			service:    "very-long-service-name-that-exceeds-kubernetes-limits-significantly",
			schema:     "very-long-schema-name",
			table:      "very-long-table-name",
			scheduleID: scheduleID,
			expected:   "very-long-service-name-that-exceeds-kubernetes-limits-significa",
		},
		{
			name:       "all numeric values",
			service:    "123",
			schema:     "456",
			table:      "789",
			scheduleID: scheduleID,
			expected:   "123-456-789-" + shortSuffix,
		},
		{
			name:       "mixed alphanumeric",
			service:    "service1",
			schema:     "schema2",
			table:      "table3",
			scheduleID: scheduleID,
			expected:   "service1-schema2-table3-" + shortSuffix,
		},
		{
			name:       "empty service/schema/table but valid scheduleID suffix",
			service:    "___",
			schema:     "...",
			table:      "!!!",
			scheduleID: scheduleID,
			expected:   shortSuffix,
		},
		{
			name:       "only special characters but valid scheduleID suffix",
			service:    "@#$",
			schema:     "%^&",
			table:      "*()",
			scheduleID: scheduleID,
			expected:   shortSuffix,
		},
		{
			name:       "unicode characters converted",
			service:    "sėrvicė",
			schema:     "schēma",
			table:      "tåble",
			scheduleID: scheduleID,
			expected:   "s-rvic-sch-ma-t-ble-" + shortSuffix,
		},
		{
			name:       "already compliant name",
			service:    "executor-controller",
			schema:     "public",
			table:      "metrics-v2",
			scheduleID: scheduleID,
			expected:   "executor-controller-public-metrics-v2-" + shortSuffix,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := pkgDomain.ComputeJobName(tt.service, tt.schema, tt.table, tt.scheduleID)

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
					if !((ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') || ch == '-') {
						t.Errorf("result %q contains invalid character: %c", result, ch)
					}
				}
			}
		})
	}
}
