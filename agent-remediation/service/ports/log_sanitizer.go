package ports

// LogSanitizer redacts a dbt log before it reaches the LLM. A pass-through
// implementation is wired at start-up; the seam allows real PII/token redaction
// to be dropped in without touching the handler.
type LogSanitizer interface {
	Sanitize(log string) string
}
