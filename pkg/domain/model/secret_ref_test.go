package model_test

import (
	"encoding/json"
	"os"
	"strings"
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

func TestValidateNodeSecretRef(t *testing.T) {
	cases := []struct {
		name     string
		nodeType model.NodeType
		ref      string
		wantErr  string
	}{
		{name: "empty ref on a type that allows one", nodeType: model.NodeTypePythonApi, ref: ""},
		{name: "empty ref on a type that allows none", nodeType: model.NodeTypePythonNode, ref: ""},
		{name: "valid ref on a type that allows one", nodeType: model.NodeTypePythonApi, ref: "continuo-api-fx"},
		{name: "ref on python-node", nodeType: model.NodeTypePythonNode, ref: "continuo-api-fx", wantErr: `"python-node"`},
		{name: "ref on dbt-model", nodeType: model.NodeTypeDbtModel, ref: "continuo-api-fx", wantErr: `"dbt-model"`},
		{name: "ref outside the prefix", nodeType: model.NodeTypePythonApi, ref: "continuo-app-credentials", wantErr: model.ApiSecretRefPrefix},
		{name: "uppercase ref", nodeType: model.NodeTypePythonApi, ref: "continuo-api-FX", wantErr: model.ApiSecretRefPrefix},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := model.ValidateNodeSecretRef(tc.nodeType, tc.ref)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("ValidateNodeSecretRef(%q, %q) = %v, want nil", tc.nodeType, tc.ref, err)
				}
				return
			}
			if err == nil {
				t.Fatalf("ValidateNodeSecretRef(%q, %q) = nil, want an error", tc.nodeType, tc.ref)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("error %q does not name %s", err, tc.wantErr)
			}
		})
	}
}

// TestValidateNodeSecretRef_OnlyAllowingTypesAccept pins the kind rule to the
// generated AllowsSecretRef: every type the vocabulary allows accepts a valid
// ref, every other type rejects it.
func TestValidateNodeSecretRef_OnlyAllowingTypesAccept(t *testing.T) {
	for _, nt := range model.NodeTypes() {
		err := model.ValidateNodeSecretRef(nt, "continuo-api-fx")
		if nt.AllowsSecretRef() != (err == nil) {
			t.Errorf("%s: AllowsSecretRef()=%v but ValidateNodeSecretRef err=%v", nt, nt.AllowsSecretRef(), err)
		}
	}
}
