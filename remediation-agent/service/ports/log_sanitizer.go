package ports

// LogSanitizer scrubs likely warehouse-data values from a dbt log before it
// reaches the LLM, while preserving the error structure (error classes,
// relation/column identifiers, LINE markers, node ids, SQLSTATE codes) the LLM
// needs to diagnose the failure. The seam keeps redaction policy out of the
// handler.
type LogSanitizer interface {
	Sanitize(log string) string
}
