package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"
)

var (
	reInvalidChars       = regexp.MustCompile(`[^a-z0-9-]`)
	reConsecutiveHyphens = regexp.MustCompile(`-+`)
)

// cutHashLen is how many hex characters of the full prefix's SHA-256 a name
// carries when its prefix is cut to fit.
const cutHashLen = 6

// ComputeJobName generates the Kubernetes Job name of one attempt of a task:
// the sanitized {service}-{schema}-{table} prefix, the first 8 characters of
// the run id (runID), and -r{attempt} for every attempt after the first.
// It follows DNS-1123 label rules: lowercase alphanumerics and hyphens,
// starting and ending alphanumeric, at most 63 characters.
//
// Format: {prefix}-{runID[:8]} for attempt 0, {prefix}-{runID[:8]}-r{attempt}
// after it.
//
// The prefix is lowercased, invalid characters become hyphens, runs of hyphens
// collapse and leading and trailing hyphens are stripped. It is then clamped so
// the run-id suffix and the attempt suffix always fit whole, and a hyphen the
// clamp leaves at its end is trimmed. With a full 8-character run id the prefix
// has room for 54 characters at attempt 0, 51 at attempts 1-9 and 50 at 10-99.
// A prefix longer than its room keeps the first 47 characters (44 at attempts
// 1-9, 43 at 10-99) and ends with -{hash}, the first 6 hex characters of the
// full prefix's SHA-256, so two nodes whose prefixes differ only past the cut
// keep distinct names at every attempt: the versions of one dbt model
// ("<name>.v1", "<name>.v2") differ only at the end of the table name, and
// can run in one run. A prefix that fits is not cut and carries no hash. A
// negative attempt, or a name that is empty after sanitization, is an error.
func ComputeJobName(serviceName, schemaName, tableName, runID string, attempt int) (string, error) {
	if attempt < 0 {
		return "", fmt.Errorf("attempt %d is negative", attempt)
	}
	attemptSuffix := ""
	if attempt > 0 {
		attemptSuffix = fmt.Sprintf("-r%d", attempt)
	}

	// Clamp suffix to 8 chars.
	idSuffix := runID
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
	// attempt 0, 51 at attempts 1-9 and 50 at attempts 10-99.
	if idSuffix != "" {
		maxPrefixLen := 63 - 1 - len(idSuffix) - len(attemptSuffix)
		if len(prefix) > maxPrefixLen {
			sum := sha256.Sum256([]byte(prefix))
			tag := hex.EncodeToString(sum[:])[:cutHashLen]
			prefix = strings.TrimRight(prefix[:maxPrefixLen-1-cutHashLen], "-") + "-" + tag
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
