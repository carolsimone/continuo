// Package main is a placeholder GitHub API stub used in e2e tests.
// It listens on :9200 and will be fleshed out in Task 10 with canned
// responses for the GitHub Contents API so the remediation-agent's
// source-reader adapter can be exercised without a real GitHub token.
package main

import (
	"net/http"
)

func main() {
	http.ListenAndServe(":9200", nil)
}
