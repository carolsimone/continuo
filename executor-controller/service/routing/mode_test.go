package routing_test

import (
	"testing"

	"github.com/carolsimone/continuo/executor-controller/domain/model"
	"github.com/carolsimone/continuo/executor-controller/service/routing"
	pkgmodel "github.com/carolsimone/continuo/pkg/domain/model"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// completeRef is a well-formed runtime manifest reference: every field set, the
// URI an s3:// location, and both digests lowercase SHA-256 hex.
func completeRef() pkgmodel.RuntimeManifestRef {
	return pkgmodel.RuntimeManifestRef{
		RuntimeManifestURI:                "s3://artifacts/finance/partial_parse.msgpack",
		RuntimeManifestSHA256:             "a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90",
		RuntimeManifestDBTVersion:         "1.12.0b1",
		RuntimeManifestParseContextSHA256: "0f1e2d3c4b5a69788796a5b4c3d2e1f00f1e2d3c4b5a69788796a5b4c3d2e1f0",
	}
}

func TestModePolicy(t *testing.T) {
	policy := routing.NewPolicy(model.ExecutionModeWorkers,
		map[string]model.ExecutionMode{"legacy": model.ExecutionModeJobs})

	assert.Equal(t, model.ExecutionModeJobs,
		policy.Resolve("finance", "", "", pkgmodel.RuntimeManifestRef{}),
		"a historical message carries no dbt_unique_id and stays on the Jobs path")
	assert.Equal(t, model.ExecutionModeJobs,
		policy.Resolve("legacy", "", "model.legacy.orders", completeRef()),
		"a service pinned to jobs stays on the Jobs path")
	assert.Equal(t, model.ExecutionModeJobs,
		policy.Resolve("finance", pkgevents.ModePromoteSeed,
			"seed.finance.currency", completeRef()),
		"promote-seed dispatch has no worker path")
	assert.Equal(t, model.ExecutionModeWorkers,
		policy.Resolve("finance", "", "model.finance.orders", completeRef()))
	assert.Error(t,
		policy.Validate("finance", "", "model.finance.orders", pkgmodel.RuntimeManifestRef{}),
		"a migrated node routed to workers without a runtime manifest cannot be dispatched")
}

// TestPolicy_JobsDefaultIsInert pins that the shipped default routes every
// record to the Jobs path, whatever the record carries.
func TestPolicy_JobsDefaultIsInert(t *testing.T) {
	policy := routing.NewPolicy(model.ExecutionModeJobs, nil)

	assert.Equal(t, model.ExecutionModeJobs,
		policy.Resolve("finance", "", "model.finance.orders", completeRef()))
	assert.NoError(t,
		policy.Validate("finance", "", "model.finance.orders", pkgmodel.RuntimeManifestRef{}),
		"an incomplete reference is only an error for a service routed to workers")
}

// TestPolicy_OverrideEnablesAWorkerCanary pins the rollout lever: workers reach
// exactly the services named in the override map while the default stays jobs.
func TestPolicy_OverrideEnablesAWorkerCanary(t *testing.T) {
	policy := routing.NewPolicy(model.ExecutionModeJobs,
		map[string]model.ExecutionMode{"finance": model.ExecutionModeWorkers})

	assert.Equal(t, model.ExecutionModeWorkers,
		policy.Resolve("finance", "", "model.finance.orders", completeRef()))
	assert.Equal(t, model.ExecutionModeJobs,
		policy.Resolve("sales", "", "model.sales.leads", completeRef()),
		"a service outside the canary keeps the default mode")
}

// TestPolicy_IncompleteRefOnAWorkerServiceIsNotAFallback pins that a migrated
// node whose reference is half-filled fails rather than quietly running as a
// Jobs-path full-project parse.
func TestPolicy_IncompleteRefOnAWorkerServiceIsNotAFallback(t *testing.T) {
	policy := routing.NewPolicy(model.ExecutionModeWorkers, nil)

	partial := completeRef()
	partial.RuntimeManifestSHA256 = ""

	assert.Equal(t, model.ExecutionModeWorkers,
		policy.Resolve("finance", "", "model.finance.orders", partial),
		"routing still selects workers so the record is rejected, not silently downgraded")
	assert.Error(t, policy.Validate("finance", "", "model.finance.orders", partial))
}

// TestPolicy_ValidateAcceptsRecordsThatNeverReachAWorker pins that the records
// routed to Jobs — historical, promote-seed, and jobs-pinned services — are not
// held to the runtime-manifest requirement.
func TestPolicy_ValidateAcceptsRecordsThatNeverReachAWorker(t *testing.T) {
	policy := routing.NewPolicy(model.ExecutionModeWorkers,
		map[string]model.ExecutionMode{"legacy": model.ExecutionModeJobs})

	assert.NoError(t, policy.Validate("finance", "", "", pkgmodel.RuntimeManifestRef{}))
	assert.NoError(t, policy.Validate("legacy", "", "model.legacy.orders", pkgmodel.RuntimeManifestRef{}))
	assert.NoError(t, policy.Validate("finance", pkgevents.ModePromoteSeed,
		"seed.finance.currency", pkgmodel.RuntimeManifestRef{}))
}

// TestPolicy_ValidateAcceptsACompleteReference is the positive counterpart to
// the rejection cases.
func TestPolicy_ValidateAcceptsACompleteReference(t *testing.T) {
	policy := routing.NewPolicy(model.ExecutionModeWorkers, nil)
	require.NoError(t, policy.Validate("finance", "", "model.finance.orders", completeRef()))
}
