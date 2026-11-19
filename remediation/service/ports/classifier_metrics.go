package ports

// ClassifierMetrics counts what the failure classifier decides.
type ClassifierMetrics interface {
	// Classified counts one newly recorded decision: decision is the
	// classifier's routing outcome (emit or drop) and rule the rule that
	// matched, in the classifier's family:detail form.
	Classified(decision, rule string)
}

// NopClassifierMetrics counts nothing.
type NopClassifierMetrics struct{}

var _ ClassifierMetrics = NopClassifierMetrics{}

func (NopClassifierMetrics) Classified(string, string) {}
