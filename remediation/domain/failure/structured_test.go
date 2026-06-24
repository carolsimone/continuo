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
