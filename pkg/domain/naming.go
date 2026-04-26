package domain

import (
	"fmt"
	"regexp"
	"strings"
)

var (
	reInvalidChars       = regexp.MustCompile(`[^a-z0-9-]`)
	reConsecutiveHyphens = regexp.MustCompile(`-+`)
)

// ComputeJobName generates a Kubernetes-compliant job name from service/schema/table components
// plus the first 8 characters of a scheduleID to prevent name collisions across runs.
// It follows DNS-1123 subdomain naming rules which require:
// - lowercase alphanumeric characters or hyphens only
// - must start and end with an alphanumeric character
// - maximum 63 characters in length
//
// Format: {service}-{schema}-{table}-{scheduleID[:8]} (lowercase, sanitized)
//
// The function performs the following transformations:
// 1. Sanitizes the {service}-{schema}-{table} prefix (lowercase, invalid chars → hyphens,
//    consecutive hyphens collapsed, leading/trailing hyphens stripped)
// 2. Clamps the prefix to 63 - 1 - len(idSuffix) characters so the suffix always fits
// 3. Trims any trailing hyphen created by mid-hyphen truncation
// 4. Assembles {prefix}-{idSuffix} (falls back to suffix-only if prefix is empty)
// 5. Validates the result is non-empty
//
// Returns an error if the computed name is empty after sanitization.
func ComputeJobName(serviceName, schemaName, tableName, scheduleID string) (string, error) {
	// Clamp suffix to 8 chars.
	idSuffix := scheduleID
	if len(idSuffix) > 8 {
		idSuffix = idSuffix[:8]
	}

	// Sanitize only the prefix so truncation never eats the suffix.
	prefixRaw := fmt.Sprintf("%s-%s-%s", serviceName, schemaName, tableName)
	prefixRaw = strings.ToLower(prefixRaw)
	prefix := reInvalidChars.ReplaceAllString(prefixRaw, "-")
	prefix = reConsecutiveHyphens.ReplaceAllString(prefix, "-")
	prefix = strings.Trim(prefix, "-")

	// Reserve room for "-{idSuffix}" so the suffix always appears in the result.
	// maxPrefixLen = 63 - 1 - len(idSuffix); for a full 8-char suffix this equals 54.
	if idSuffix != "" {
		maxPrefixLen := 63 - 1 - len(idSuffix)
		if len(prefix) > maxPrefixLen {
			prefix = prefix[:maxPrefixLen]
			prefix = strings.TrimRight(prefix, "-")
		}
	}

	var sanitized string
	switch {
	case prefix == "" && idSuffix == "":
		// nothing left after sanitization
	case prefix == "":
		sanitized = idSuffix
	case idSuffix == "":
		sanitized = prefix
	default:
		sanitized = prefix + "-" + idSuffix
	}

	if sanitized == "" {
		return "", fmt.Errorf("computed job_name is empty after sanitization")
	}

	return sanitized, nil
}
