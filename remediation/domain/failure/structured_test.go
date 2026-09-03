package failure

import "testing"

func TestClassifyWithStructured_FailIsTest(t *testing.T) {
	sr := &StructuredResult{Status: "fail", Message: "Failure in test not_null_x (models/x.sql)", Failures: 14}
	c := ClassifyWithStructured(sr, "")
	if c.Category != CategoryTest {
		t.Fatalf("category = %s, want test", c.Category)
	}
}

func TestClassifyWithStructured_ErrorUsesMessageRules(t *testing.T) {
	sr := &StructuredResult{Status: "error", Message: `relation "x" does not exist`}
	c := ClassifyWithStructured(sr, "ignored log text")
	if c.Category != CategoryLogic {
		t.Fatalf("category = %s, want logic", c.Category)
	}
}

func TestClassifyWithStructured_NilFallsBackToLog(t *testing.T) {
	c := ClassifyWithStructured(nil, "connection refused")
	if c.Category != CategoryInfraTransient {
		t.Fatalf("category = %s, want infra_transient (log fallback)", c.Category)
	}
}

func TestClassifyWithStructured_ErrorEmptyMessageFallsBackToLog(t *testing.T) {
	sr := &StructuredResult{Status: "error", Message: ""}
	c := ClassifyWithStructured(sr, "connection refused")
	if c.Category != CategoryInfraTransient {
		t.Fatalf("category = %s, want infra_transient (empty structured message → log fallback)", c.Category)
	}
}
