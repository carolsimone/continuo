package failure

import "testing"

func TestParseStructuredResult(t *testing.T) {
	sr, err := ParseStructuredResult([]byte(`{"schema_version":1,"status":"error","message":"relation \"x\" does not exist","failures":0,"unique_id":"model.svc.x"}`))
	if err != nil {
		t.Fatal(err)
	}
	if sr.Status != "error" || sr.UniqueID != "model.svc.x" {
		t.Fatalf("parsed = %+v", sr)
	}
}

func TestParseStructuredResult_StderrPreamble(t *testing.T) {
	// The validation pod's uploaded object can carry a stderr/log preamble before
	// the JSON contract. A strict whole-body unmarshal fails on it, which made the
	// classifier degrade to the text log and emit "unknown" for genuine logic
	// failures (issue #203). The parser must locate the JSON object instead.
	raw := []byte("Running with dbt=1.12.0b1\n[WARNING]: deprecated config\n" +
		`{"schema_version":1,"status":"error","message":"relation \"x\" does not exist","failures":0,"unique_id":"model.svc.x"}` + "\n")
	sr, err := ParseStructuredResult(raw)
	if err != nil {
		t.Fatal(err)
	}
	if sr.Status != "error" || sr.UniqueID != "model.svc.x" {
		t.Fatalf("parsed = %+v", sr)
	}
}

func TestParseStructuredResult_BraceInPreambleAndTrailing(t *testing.T) {
	// A '{' in the preamble must not derail the parser, and trailing output after
	// the JSON object must be ignored.
	raw := []byte("log line with {a brace} that is not json\n" +
		`{"status":"error","message":"compilation error in model","unique_id":"m.x"}` + "\ntrailing stderr noise\n")
	sr, err := ParseStructuredResult(raw)
	if err != nil {
		t.Fatal(err)
	}
	if sr.Status != "error" || sr.Message != "compilation error in model" {
		t.Fatalf("parsed = %+v", sr)
	}
}

func TestParseStructuredResult_NoJSONIsError(t *testing.T) {
	// A body with no decodable structured object is still a real error so the
	// caller falls back to the text log rather than misclassifying.
	if _, err := ParseStructuredResult([]byte("just stderr, no json object here\n")); err == nil {
		t.Fatal("expected error for body with no JSON object")
	}
}

func TestClassifyWithStructured_FailIsTest(t *testing.T) {
	sr := &StructuredResult{Status: "fail", Message: "Failure in test not_null_x (models/x.sql)", Failures: 14}
	c := ClassifyWithStructured(FailureEvidence{}, sr, "")
	if c.Category != CategoryTest {
		t.Fatalf("category = %s, want test", c.Category)
	}
}

func TestClassifyWithStructured_ErrorUsesMessageRules(t *testing.T) {
	sr := &StructuredResult{Status: "error", Message: `relation "x" does not exist`}
	c := ClassifyWithStructured(FailureEvidence{}, sr, "ignored log text")
	if c.Category != CategoryLogic {
		t.Fatalf("category = %s, want logic", c.Category)
	}
}

func TestClassifyWithStructured_NilFallsBackToLog(t *testing.T) {
	c := ClassifyWithStructured(FailureEvidence{}, nil, "connection refused")
	if c.Category != CategoryInfraTransient {
		t.Fatalf("category = %s, want infra_transient (log fallback)", c.Category)
	}
}

func TestClassifyWithStructured_ErrorEmptyMessageFallsBackToLog(t *testing.T) {
	sr := &StructuredResult{Status: "error", Message: ""}
	c := ClassifyWithStructured(FailureEvidence{}, sr, "connection refused")
	if c.Category != CategoryInfraTransient {
		t.Fatalf("category = %s, want infra_transient (empty structured message → log fallback)", c.Category)
	}
}
