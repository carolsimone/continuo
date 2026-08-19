package validationresult

import (
	"strings"
	"testing"
)

func TestSplit(t *testing.T) {
	log := "line a\nline b\n" +
		SentinelBegin + "\n" +
		`{"schema_version":1,"status":"error","message":"boom","failures":0,"unique_id":"model.svc.x"}` + "\n" +
		SentinelEnd + "\n"
	clean, structured := Split(log)
	if strings.Contains(clean, "CONTINUO_VALIDATION_RESULT") {
		t.Fatalf("clean log still contains sentinel: %q", clean)
	}
	if !strings.Contains(structured, `"status":"error"`) {
		t.Fatalf("structured JSON not extracted: %q", structured)
	}

	clean2, structured2 := Split("just a normal log\n")
	if structured2 != "" {
		t.Fatalf("expected no structured block, got %q", structured2)
	}
	if clean2 != "just a normal log\n" {
		t.Fatalf("clean log altered when no block present: %q", clean2)
	}
}

func TestParse(t *testing.T) {
	r, err := Parse([]byte(`{"schema_version":1,"status":"error","message":"relation \"x\" does not exist","failures":0,"unique_id":"model.svc.x"}`))
	if err != nil {
		t.Fatal(err)
	}
	if r.Status != "error" || r.UniqueID != "model.svc.x" {
		t.Fatalf("parsed = %+v", r)
	}
}

// TestParse_StderrPreamble guards the #203 stderr-preamble regression: the
// validation pod's uploaded object can carry a stderr/log preamble before the
// JSON contract. A strict whole-body unmarshal fails on it, which made a
// classifier reading this contract degrade to the text log and emit
// "unknown" for genuine logic failures. Parse must locate the JSON object
// instead of unmarshalling the whole body.
func TestParse_StderrPreamble(t *testing.T) {
	raw := []byte("Running with dbt=1.12.0b1\n[WARNING]: deprecated config\n" +
		`{"schema_version":1,"status":"error","message":"relation \"x\" does not exist","failures":0,"unique_id":"model.svc.x"}` + "\n")
	r, err := Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	if r.Status != "error" || r.UniqueID != "model.svc.x" {
		t.Fatalf("parsed = %+v", r)
	}
}

// TestParse_BraceInPreambleAndTrailing verifies a '{' in the preamble does
// not derail the scan, and trailing output after the JSON object is ignored.
func TestParse_BraceInPreambleAndTrailing(t *testing.T) {
	raw := []byte("log line with {a brace} that is not json\n" +
		`{"schema_version":1,"status":"error","message":"compilation error in model","unique_id":"m.x"}` + "\ntrailing stderr noise\n")
	r, err := Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	if r.Status != "error" || r.Message != "compilation error in model" {
		t.Fatalf("parsed = %+v", r)
	}
}

// TestParse_NoJSONIsError verifies a body with no decodable structured object
// is a real error, so the caller falls back to the text log rather than
// misclassifying.
func TestParse_NoJSONIsError(t *testing.T) {
	if _, err := Parse([]byte("just stderr, no json object here\n")); err == nil {
		t.Fatal("expected error for body with no JSON object")
	}
}

// TestParse_DecoyStatusBearingPreambleIgnored guards the contract-validation
// guard: because the scan tolerates arbitrary preamble text, an unrelated
// status-bearing JSON object in that preamble (e.g. a sidecar diagnostic with
// no schema_version field) must not be mistaken for the contract's result
// block. The real, schema_version-carrying block must win.
func TestParse_DecoyStatusBearingPreambleIgnored(t *testing.T) {
	raw := []byte("running node analytics.orders -> analytics.orders\n" +
		`{"status":"error","message":"sidecar: connection reset"}` + "\n" +
		`{"schema_version":1,"status":"error","message":"ConformError: column 'id' cannot be safely cast to INTEGER","failures":1,"unique_id":"analytics.orders"}` + "\n")
	r, err := Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	want := "ConformError: column 'id' cannot be safely cast to INTEGER"
	if r.Message != want {
		t.Fatalf("expected the real contract block's message %q, got the decoy or something else: %q", want, r.Message)
	}
}

// TestParse_DecoyWrongSchemaVersionIgnored proves schema_version specifically
// is what discriminates a real contract block from a decoy, not mere field
// presence: a decoy carrying a well-formed but wrong schema_version must
// still be skipped in favor of the real (schema_version:1) block.
func TestParse_DecoyWrongSchemaVersionIgnored(t *testing.T) {
	raw := []byte("running node analytics.orders -> analytics.orders\n" +
		`{"schema_version":2,"status":"error","message":"decoy: wrong schema version"}` + "\n" +
		`{"schema_version":1,"status":"error","message":"ConformError: column 'id' cannot be safely cast to INTEGER","failures":1,"unique_id":"analytics.orders"}` + "\n")
	r, err := Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	want := "ConformError: column 'id' cannot be safely cast to INTEGER"
	if r.Message != want {
		t.Fatalf("expected the schema_version:1 block's message %q, got %q", want, r.Message)
	}
}

// TestParse_UnsupportedStatusIgnored verifies a candidate with a
// schema_version match but a status outside the contract's vocabulary is
// rejected and the scan continues — schema_version alone is not sufficient.
func TestParse_UnsupportedStatusIgnored(t *testing.T) {
	raw := []byte(`{"schema_version":1,"status":"bogus","message":"not a real status"}` + "\n" +
		`{"schema_version":1,"status":"error","message":"the real failure"}` + "\n")
	r, err := Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	if r.Message != "the real failure" {
		t.Fatalf("expected the supported-status block's message, got %q", r.Message)
	}
}
