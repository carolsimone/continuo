package domain

import (
	"fmt"
	"regexp"
	"strings"
)

// ComputeJobName generates a Kubernetes-compliant job name from service/schema/table components.
// It follows DNS-1123 subdomain naming rules which require:
// - lowercase alphanumeric characters or hyphens only
// - must start and end with an alphanumeric character
// - maximum 63 characters in length
//
// Format: {service}-{schema}-{table} (lowercase, sanitized)
//
// The function performs the following transformations:
// 1. Combines service, schema, and table with hyphens
// 2. Converts to lowercase
// 3. Replaces invalid characters with hyphens
// 4. Collapses consecutive hyphens into a single hyphen
// 5. Removes leading and trailing hyphens
// 6. Truncates to 63 characters if necessary
// 7. Validates the result is non-empty
//
// Returns an error if the computed name is empty after sanitization.
func ComputeJobName(serviceName, schemaName, tableName string) (string, error) {
	// Combine with hyphens
	raw := fmt.Sprintf("%s-%s-%s", serviceName, schemaName, tableName)

	// Convert to lowercase
	raw = strings.ToLower(raw)

	// Replace invalid characters with hyphens (only alphanumeric and hyphens allowed)
	sanitized := regexp.MustCompile(`[^a-z0-9-]`).ReplaceAllString(raw, "-")

	// Remove consecutive hyphens
	sanitized = regexp.MustCompile(`-+`).ReplaceAllString(sanitized, "-")

	// Trim leading/trailing hyphens
	sanitized = strings.Trim(sanitized, "-")

	// Enforce 63 character limit
	if len(sanitized) > 63 {
		sanitized = sanitized[:63]
		// Trim trailing hyphen if truncation created one
		sanitized = strings.TrimRight(sanitized, "-")
	}

	// Validate non-empty
	if sanitized == "" {
		return "", fmt.Errorf("computed job_name is empty after sanitization")
	}

	return sanitized, nil
}
