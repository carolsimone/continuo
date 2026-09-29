package model

import (
	"fmt"
	"regexp"
)

// ApiSecretRefPrefix is the name prefix every Secret a python-api contract may
// name must carry. It keeps a contract from naming any other Secret in the
// namespace, such as the warehouse or platform credentials.
const ApiSecretRefPrefix = "continuo-api-"

// apiSecretRefMaxLen is the Kubernetes Secret name limit (a DNS subdomain).
const apiSecretRefMaxLen = 253

var apiSecretRefPattern = regexp.MustCompile(`^continuo-api-[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)

// ValidateApiSecretRef reports whether ref is a Secret name a python-api node
// may receive. topology-controller holds the same rule; both are pinned to
// testdata/secret_ref_cases.json.
func ValidateApiSecretRef(ref string) error {
	if len(ref) > apiSecretRefMaxLen || !apiSecretRefPattern.MatchString(ref) {
		return fmt.Errorf("secret_ref %q must be a Kubernetes Secret name starting with %q "+
			"(lowercase letters, digits, '-'; at most %d chars)", ref, ApiSecretRefPrefix, apiSecretRefMaxLen)
	}
	return nil
}
