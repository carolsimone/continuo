package deploycheck

import (
	"encoding/json"
	"regexp"
	"slices"
	"testing"

	pkgconfig "github.com/carolsimone/continuo/pkg/config"
	"gopkg.in/yaml.v3"
)

// TestCredentialVarsMatchTheChart pins pkgconfig.CredentialVars, the closed
// list of variables a service reads through Secret (each as the variable or
// as <NAME>_FILE), to the credentials the chart hands to services: every
// secretEnv name of the chart's services, plus the S3 keys, which the
// deployment template adds to every storage-reading service on its own. A
// credential the chart starts delivering without a Secret read would reach
// the service only in its environment form, and a Secret read the chart never
// delivers is dead. The chart's helper continuo.credentialVariables, the list
// credentials.files keys are checked against and file delivery mounts, must
// hold the same names.
func TestCredentialVarsMatchTheChart(t *testing.T) {
	var values struct {
		Services []struct {
			Name      string `yaml:"name"`
			SecretEnv []struct {
				Name string `yaml:"name"`
			} `yaml:"secretEnv"`
		} `yaml:"services"`
	}
	if err := yaml.Unmarshal([]byte(readRepoFile(t, "deploy", "continuo", "values.yaml")), &values); err != nil {
		t.Fatalf("parse values.yaml: %v", err)
	}
	if len(values.Services) == 0 {
		t.Fatal("values.yaml lists no services")
	}
	chart := []string{"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"}
	for _, svc := range values.Services {
		for _, env := range svc.SecretEnv {
			chart = append(chart, env.Name)
		}
	}
	slices.Sort(chart)
	chart = slices.Compact(chart)

	want := slices.Clone(pkgconfig.CredentialVars)
	slices.Sort(want)
	if !slices.Equal(chart, want) {
		t.Fatalf("the chart's secretEnv names plus the S3 keys are %v; pkgconfig.CredentialVars is %v", chart, want)
	}

	helper := chartCredentialVariables(t)
	slices.Sort(helper)
	if !slices.Equal(helper, want) {
		t.Fatalf("_helpers.tpl continuo.credentialVariables is %v; pkgconfig.CredentialVars is %v", helper, want)
	}
}

// credentialVariablesDefine matches the body of the continuo.credentialVariables
// define in _helpers.tpl, a JSON array of variable names.
var credentialVariablesDefine = regexp.MustCompile(`(?s)\{\{-? define "continuo\.credentialVariables" -?\}\}(.*?)\{\{-? end -?\}\}`)

// chartCredentialVariables returns the names the chart's
// continuo.credentialVariables helper lists.
func chartCredentialVariables(t *testing.T) []string {
	t.Helper()
	m := credentialVariablesDefine.FindStringSubmatch(readRepoFile(t, "deploy", "continuo", "templates", "_helpers.tpl"))
	if m == nil {
		t.Fatal(`_helpers.tpl has no define "continuo.credentialVariables"`)
	}
	var names []string
	if err := json.Unmarshal([]byte(m[1]), &names); err != nil {
		t.Fatalf("parse continuo.credentialVariables %q: %v", m[1], err)
	}
	if len(names) == 0 {
		t.Fatal("continuo.credentialVariables lists no variables")
	}
	return names
}
