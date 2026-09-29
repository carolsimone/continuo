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

func TestValidateApiSecretRef_LengthBoundaries(t *testing.T) {
	c := loadSecretRefCases(t)
	if len(c.Valid) == 0 || len(c.Invalid) == 0 {
		t.Fatalf("fixture must carry both lists, has %d valid and %d invalid", len(c.Valid), len(c.Invalid))
	}
	atLimit := c.Valid[len(c.Valid)-1]
	if len(atLimit) != 253 {
		t.Fatalf("the last valid case must be the 253-char limit, is %d", len(atLimit))
	}
	overLimit := c.Invalid[len(c.Invalid)-1]
	if len(overLimit) != 254 {
		t.Fatalf("the last invalid case must be 254 chars, is %d", len(overLimit))
	}
}
