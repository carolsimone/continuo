package model

import (
	"fmt"
	"regexp"
)

// ApiSecretRefPrefix is the name prefix every Secret a python-api contract may
// name must carry. The prefix is reserved for these operator-created Secrets:
// the continuo Helm chart refuses to install when its own name, or any Secret
// name it creates or references, falls inside it, so a contract cannot name
// the warehouse or platform credentials the chart manages.
const ApiSecretRefPrefix = "continuo-api-"

// apiSecretRefMaxLen is the Kubernetes Secret name limit (a DNS subdomain).
const apiSecretRefMaxLen = 253

var apiSecretRefPattern = regexp.MustCompile("^" + regexp.QuoteMeta(ApiSecretRefPrefix) + "[a-z0-9]([-a-z0-9]*[a-z0-9])?$")

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

// ValidateNodeSecretRef reports whether a node of nodeType may carry ref. An
// empty ref is always allowed; a non-empty one requires a node type whose
// contract may name a Secret (NodeType.AllowsSecretRef) and a name that passes
// ValidateApiSecretRef.
func ValidateNodeSecretRef(nodeType NodeType, ref string) error {
	if ref == "" {
		return nil
	}
	if !nodeType.AllowsSecretRef() {
		return fmt.Errorf("secret_ref %q is not allowed on node type %q", ref, nodeType)
	}
	return ValidateApiSecretRef(ref)
}
