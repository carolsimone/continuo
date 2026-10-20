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

// ComputeJobName generates the Kubernetes Job name of one attempt of a task:
// the sanitized {service}-{schema}-{table} prefix, the first 8 characters of
// the run id (scheduleID), and -r{attempt} for every attempt after the first.
// It follows DNS-1123 label rules: lowercase alphanumerics and hyphens,
// starting and ending alphanumeric, at most 63 characters.
//
// Format: {prefix}-{scheduleID[:8]} for attempt 0, {prefix}-{scheduleID[:8]}-r{attempt}
// after it.
//
// The prefix is lowercased, invalid characters become hyphens, runs of hyphens
// collapse and leading and trailing hyphens are stripped. It is then clamped so
// the run-id suffix and the attempt suffix always fit whole, and a hyphen the
// clamp leaves at its end is trimmed. A negative attempt, or a name that is
// empty after sanitization, is an error.
func ComputeJobName(serviceName, schemaName, tableName, scheduleID string, attempt int) (string, error) {
	if attempt < 0 {
		return "", fmt.Errorf("attempt %d is negative", attempt)
	}
	attemptSuffix := ""
	if attempt > 0 {
		attemptSuffix = fmt.Sprintf("-r%d", attempt)
	}

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

	// Reserve room for "-{idSuffix}" and the attempt suffix so both always
	// appear whole: for a full 8-char run id, 54 characters of prefix at
	// attempt 0, 51 at attempts 1-9.
	if idSuffix != "" {
		maxPrefixLen := 63 - 1 - len(idSuffix) - len(attemptSuffix)
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

	return sanitized + attemptSuffix, nil
}
