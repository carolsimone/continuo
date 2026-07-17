package k8s

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

// A missing RBAC verb cannot fail until a pool is enabled, and then it fails in
// the cluster rather than in a test. So rather than restate the verbs by hand,
// these tests drive the runtime against a fake cluster, record the API calls it
// actually makes, and hold each deployed Role to exactly those.
//
// The consequence is that granting a verb is never enough on its own: a call
// added to the runtime shows up here as an ungranted action, and a Role that
// drifts behind the code fails before the cluster ever sees it.

// grant is one (apiGroup, resource, verb) the executor is allowed.
type grant struct{ apiGroup, resource, verb string }

// rbacManifests are the manifests carrying the executor's Role, and how to find
// its rules in each.
var rbacManifests = []string{
	"deploy/app/values.yaml",
	"deploy/continuo/values.yaml",
	"tests/e2e/k8s/executor-controller-deployment.yaml",
}

func rbacRepoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	require.NoError(t, err)
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.work")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		require.NotEqual(t, parent, dir, "go.work not found above the test directory")
		dir = parent
	}
}

// rules is a Role's rules as every manifest spells them.
type rules []struct {
	APIGroups []string `yaml:"apiGroups"`
	Resources []string `yaml:"resources"`
	Verbs     []string `yaml:"verbs"`
}

// granted expands a manifest's rules into the set of allowed calls.
func (r rules) granted() map[grant]bool {
	out := map[grant]bool{}
	for _, rule := range r {
		for _, group := range rule.APIGroups {
			for _, resource := range rule.Resources {
				for _, verb := range rule.Verbs {
					out[grant{group, resource, verb}] = true
				}
			}
		}
	}
	return out
}

// executorRBAC reads the executor's Role from one manifest.
func executorRBAC(t *testing.T, rel string) map[grant]bool {
	t.Helper()
	//nolint:gosec // G304: rel is rbacRepoRoot(t) (discovered by walking up for go.work) joined with a fixed literal manifest path, not external input
	body, err := os.ReadFile(filepath.Join(rbacRepoRoot(t), rel))
	require.NoError(t, err)

	if strings.HasSuffix(rel, "values.yaml") {
		var file struct {
			Services []struct {
				Name string `yaml:"name"`
				RBAC struct {
					Rules rules `yaml:"rules"`
				} `yaml:"rbac"`
			} `yaml:"services"`
		}
		require.NoError(t, yaml.Unmarshal(body, &file), rel)
		for _, svc := range file.Services {
			if svc.Name == "executor-controller" {
				return svc.RBAC.Rules.granted()
			}
		}
		t.Fatalf("%s has no executor-controller service", rel)
	}

	for _, doc := range strings.Split(string(body), "\n---") {
		var manifest struct {
			Kind     string `yaml:"kind"`
			Metadata struct {
				Name string `yaml:"name"`
			} `yaml:"metadata"`
			Rules rules `yaml:"rules"`
		}
		require.NoError(t, yaml.Unmarshal([]byte(doc), &manifest), rel)
		if manifest.Kind == "Role" && manifest.Metadata.Name == "executor-controller" {
			return manifest.Rules.granted()
		}
	}
	t.Fatalf("%s has no executor-controller Role", rel)
	return nil
}

// requiredGrants drives every call the pool runtime makes and reports the RBAC
// they need, read from what the fake cluster was actually asked to do.
func requiredGrants(t *testing.T) map[grant]bool {
	t.Helper()
	pools, cs := newTestWorkerPools()
	ctx := context.Background()

	// Creating a pool, then reconciling it again so the update paths run too.
	require.NoError(t, pools.Ensure(ctx, testSpec()))
	require.NoError(t, pools.Ensure(ctx, testSpec()))
	_, _, err := pools.Status(ctx, testPoolKey)
	require.NoError(t, err)
	require.NoError(t, pools.DeletePod(ctx, "dbt-worker-pod", testPoolKey))

	required := map[grant]bool{}
	for _, action := range cs.Actions() {
		gvr := action.GetResource()
		required[grant{gvr.Group, gvr.Resource, action.GetVerb()}] = true
	}
	require.NotEmpty(t, required, "the runtime made no API calls to derive RBAC from")
	return required
}

// TestRBAC_EveryDeployedRoleCoversWhatTheRuntimeCalls pins that each manifest
// grants every call the pool runtime makes. A verb the runtime needs and a Role
// omits would surface only once a pool was enabled, as a forbidden error in the
// cluster.
func TestRBAC_EveryDeployedRoleCoversWhatTheRuntimeCalls(t *testing.T) {
	required := requiredGrants(t)
	for _, manifest := range rbacManifests {
		t.Run(manifest, func(t *testing.T) {
			allowed := executorRBAC(t, manifest)
			for call := range required {
				if !allowed[call] {
					t.Errorf("%s does not allow %s on %q (apiGroup %q), which the "+
						"worker pool runtime calls",
						manifest, call.verb, call.resource, call.apiGroup)
				}
			}
		})
	}
}

// poolOwnedResources are the resources only the worker pools use. Jobs and pods
// are left out because the Jobs path reads them too, so a verb there is not the
// pool runtime's to justify.
var poolOwnedResources = map[string]bool{"deployments": true, "secrets": true}

// TestRBAC_NoDeployedRoleOverGrantsPoolResources pins least privilege over the
// resources the pools own: the executor may create and resize a pool's
// Deployment and write its Secret, and nothing else. Deleting either is not a
// call it makes, and a Role that allowed it would let a bug destroy a pool's
// credential or its running pods.
func TestRBAC_NoDeployedRoleOverGrantsPoolResources(t *testing.T) {
	required := requiredGrants(t)
	for _, manifest := range rbacManifests {
		t.Run(manifest, func(t *testing.T) {
			for call := range executorRBAC(t, manifest) {
				if !poolOwnedResources[call.resource] || required[call] {
					continue
				}
				t.Errorf("%s allows %s on %q, which the worker pool runtime never calls",
					manifest, call.verb, call.resource)
			}
		})
	}
}
