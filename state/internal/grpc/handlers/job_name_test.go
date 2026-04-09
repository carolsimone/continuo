package handlers

import (
	"testing"

	pkgDomain "github.com/carolsimone/continuo/pkg/domain"
)

func TestComputeJobName(t *testing.T) {
	tests := []struct {
		name        string
		service     string
		schema      string
		table       string
		expected    string
		expectError bool
	}{
		{
			name:     "simple case",
			service:  "analytics",
			schema:   "public",
			table:    "users",
			expected: "analytics-public-users",
		},
		{
			name:     "uppercase converted to lowercase",
			service:  "Analytics",
			schema:   "PUBLIC",
			table:    "Users",
			expected: "analytics-public-users",
		},
		{
			name:     "underscores converted to hyphens",
			service:  "my_service",
			schema:   "prod_schema",
			table:    "user_table",
			expected: "my-service-prod-schema-user-table",
		},
		{
			name:     "dots converted to hyphens",
			service:  "prod.service",
			schema:   "public.schema",
			table:    "user.table",
			expected: "prod-service-public-schema-user-table",
		},
		{
			name:     "special chars sanitized",
			service:  "my@service",
			schema:   "prod#schema",
			table:    "user$table",
			expected: "my-service-prod-schema-user-table",
		},
		{
			name:     "consecutive hyphens collapsed",
			service:  "my--service",
			schema:   "prod___schema",
			table:    "user..table",
			expected: "my-service-prod-schema-user-table",
		},
		{
			name:     "leading and trailing hyphens removed",
			service:  "-service",
			schema:   "_schema_",
			table:    "table-",
			expected: "service-schema-table",
		},
		{
			name:     "max length truncation at 63 chars",
			service:  "very-long-service-name-that-exceeds-kubernetes-limits-significantly",
			schema:   "very-long-schema-name",
			table:    "very-long-table-name",
			expected: "very-long-service-name-that-exceeds-kubernetes-limits-significa",
		},
		{
			name:     "truncation removes trailing hyphen",
			service:  "service-name-with-exactly-sixty-three-characters-when-combined",
			schema:   "schema",
			table:    "table",
			expected: "service-name-with-exactly-sixty-three-characters-when-combined",
		},
		{
			name:     "all numeric values",
			service:  "123",
			schema:   "456",
			table:    "789",
			expected: "123-456-789",
		},
		{
			name:     "mixed alphanumeric",
			service:  "service1",
			schema:   "schema2",
			table:    "table3",
			expected: "service1-schema2-table3",
		},
		{
			name:        "empty after sanitization",
			service:     "___",
			schema:      "...",
			table:       "!!!",
			expectError: true,
		},
		{
			name:        "only special characters",
			service:     "@#$",
			schema:      "%^&",
			table:       "*()",
			expectError: true,
		},
		{
			name:     "unicode characters converted",
			service:  "sėrvicė",
			schema:   "schēma",
			table:    "tåble",
			expected: "s-rvic-sch-ma-t-ble",
		},
		{
			name:     "already compliant name",
			service:  "executor-controller",
			schema:   "public",
			table:    "metrics-v2",
			expected: "executor-controller-public-metrics-v2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := pkgDomain.ComputeJobName(tt.service, tt.schema, tt.table)

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
