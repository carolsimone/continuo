package failure

// StructuredResult is the parsed structured validation-result contract emitted
// by the validation pod (validation_runner.py / seed wrapper) and uploaded by
// k8s-controller. It is the preferred classification signal; the text log is
// the fallback when it is absent. status uses dbt's RunStatus vocabulary. This
// is a pure domain value — decoding the wire JSON into it is the application
// layer's job (see classify_failure.go), not this package's.
type StructuredResult struct {
	Status   string
	Message  string
	Failures int
	UniqueID string
}
