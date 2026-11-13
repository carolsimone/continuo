package k8s

import (
	"encoding/json"
	"fmt"
	"os"

	validationmodel "github.com/carolsimone/continuo/execution-controller/domain/model"
	"github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/seedreload"
	corev1 "k8s.io/api/core/v1"
)

// seedPreflightInitContainer returns the init container that runs the seed
// preflight p against the warehouse: the validation image's runner with
// VALIDATION_OP=seed_preflight, the seed's relation split into PROD_SCHEMA and
// TABLE_NAME, and nodeID as NODE_ID. In check mode the runner's exit code is
// the container's, so a stop fails the init container and the pod never
// reaches dbt. The error wraps events.ErrPermanent when VALIDATION_IMAGE or
// the warehouse Secret is not configured, or the relation does not split on
// its first dot into a schema and a table.
func seedPreflightInitContainer(p seedreload.Preflight, nodeID string) (corev1.Container, error) {
	image := os.Getenv("VALIDATION_IMAGE")
	if image == "" {
		return corev1.Container{}, fmt.Errorf("%w: VALIDATION_IMAGE not configured (set it to the matching continuo-python-runtime-<engine> image) for the seed preflight of node %s",
			events.ErrPermanent, nodeID)
	}
	schema, table, ok := validationmodel.SeedRelation(p.Relation)
	if !ok {
		return corev1.Container{}, fmt.Errorf("%w: seed preflight relation %q of node %s is not <schema>.<table>",
			events.ErrPermanent, p.Relation, nodeID)
	}
	whFrom, err := warehouseSecretEnvFrom("the seed preflight of node " + nodeID)
	if err != nil {
		return corev1.Container{}, err
	}
	command := validationmodel.ValidationCommand("", "")
	if p.Mode == seedreload.PreflightReport {
		// The runner exits 2 in report mode too, on a bad input or a missing
		// adapter env. A report must never stop the load, so it runs through
		// a shell that always exits 0.
		command = []string{"sh", "-c", shellJoin(command) + "; exit 0"}
	}
	return corev1.Container{
		Name:            validationmodel.SeedPreflightContainer,
		Image:           image,
		ImagePullPolicy: validationImagePullPolicy(),
		Command:         command,
		Env: []corev1.EnvVar{
			{Name: "VALIDATION_OP", Value: "seed_preflight"},
			{Name: "SEED_PREFLIGHT_MODE", Value: string(p.Mode)},
			{Name: "SEED_PREFLIGHT_ENGINE_FAMILY", Value: string(p.Family)},
			{Name: "PROD_SCHEMA", Value: schema},
			{Name: "TABLE_NAME", Value: table},
			{Name: "SEED_PREFLIGHT_ALLOWED", Value: jsonNames(p.Allowed)},
			{Name: "SEED_PREFLIGHT_ACKNOWLEDGED", Value: jsonNames(p.Acknowledged)},
			{Name: "NODE_ID", Value: nodeID},
		},
		EnvFrom:         whFrom,
		SecurityContext: continuoImageSecurityContext(),
	}, nil
}

// jsonNames renders names as a JSON array, `[]` when names is nil or empty,
// never `null`. Encoding a list of strings cannot fail.
func jsonNames(names []string) string {
	if names == nil {
		names = []string{}
	}
	raw, _ := json.Marshal(names)
	return string(raw)
}
