package model_test

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/carolsimone/continuo/pkg/domain/model"
)

type secretRefCases struct {
	Valid   []string `json:"valid"`
	Invalid []string `json:"invalid"`
}

func loadSecretRefCases(t *testing.T) secretRefCases {
	t.Helper()
	raw, err := os.ReadFile("testdata/secret_ref_cases.json")
	if err != nil {
		t.Fatal(err)
	}
	var c secretRefCases
	if err := json.Unmarshal(raw, &c); err != nil {
		t.Fatal(err)
	}
	return c
}

func TestValidateApiSecretRef_SharedCases(t *testing.T) {
	c := loadSecretRefCases(t)
	for _, ref := range c.Valid {
		if err := model.ValidateApiSecretRef(ref); err != nil {
			t.Errorf("ValidateApiSecretRef(%q) = %v, want nil", ref, err)
		}
	}
	for _, ref := range c.Invalid {
		if err := model.ValidateApiSecretRef(ref); err == nil {
			t.Errorf("ValidateApiSecretRef(%q) = nil, want an error", ref)
		}
	}
}

func TestValidateApiSecretRef_LengthCaseIsOverTheLimit(t *testing.T) {
	c := loadSecretRefCases(t)
	long := c.Invalid[len(c.Invalid)-1]
	if len(long) != 254 {
		t.Fatalf("the length case must be 254 chars, is %d", len(long))
	}
}
