package ports

// LogSanitizer is the seam through which the legacy lanes pass a text before it
// reaches the LLM. Their implementation returns the text unchanged. The runner
// does not use it: it cleans every model input with its own Redactor.
type LogSanitizer interface {
	Sanitize(log string) string
}
