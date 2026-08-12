// Package sanitize is the single redaction seam for untrusted text the system
// stores or forwards to an LLM: dbt logs on the remediation path and model
// source code on the graph-ingestion path.
//
// Text currently returns its input unchanged. It exists as one function rather
// than a copy per service so a real redactor — credentials embedded in SQL
// literals, tokens echoed into a dbt log — is added in one place and every
// caller picks it up.
package sanitize

// Text returns the string with any redaction rules applied.
func Text(s string) string { return s }
