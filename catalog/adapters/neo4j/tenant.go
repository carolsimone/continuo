package neo4jinfra

import (
	"context"
	"errors"
	"fmt"
	"maps"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// tenantParam is the Cypher parameter every catalog statement names its
// tenant by.
const tenantParam = "tenant_id"

var (
	errEmptyTenant         = errors.New("neo4j: a catalog statement needs a tenant")
	errTenantParamOverride = errors.New("neo4j: a statement's parameters must not set tenant_id; the tenant runner binds it")
	errTenantMismatch      = errors.New("neo4j: a repository call names another tenant than its transaction's")
)

// TenantRunner runs Cypher statements for one tenant. It binds $tenant_id to
// that tenant on every statement, so a statement reaches the tenant's nodes
// only by naming tenant_id: $tenant_id in its patterns.
type TenantRunner struct {
	tenantID string
	runner   interface {
		Run(ctx context.Context, cypher string, params map[string]any) (neo4j.ResultWithContext, error)
	}
}

// NewTenantRunner returns a TenantRunner that runs statements on r for
// tenantID. r is an open transaction: neo4j.ExplicitTransaction and
// neo4j.ManagedTransaction both satisfy it. It refuses an empty tenant.
func NewTenantRunner(tenantID string, r interface {
	Run(ctx context.Context, cypher string, params map[string]any) (neo4j.ResultWithContext, error)
}) (TenantRunner, error) {
	if tenantID == "" {
		return TenantRunner{}, errEmptyTenant
	}
	if r == nil {
		return TenantRunner{}, errors.New("neo4j: a tenant runner needs a transaction to run on")
	}
	return TenantRunner{tenantID: tenantID, runner: r}, nil
}

// Run runs cypher with params and $tenant_id bound to the runner's tenant. It
// refuses params that already carry tenant_id, and never changes the caller's
// map. The zero TenantRunner carries no tenant and refuses every statement.
func (t TenantRunner) Run(ctx context.Context, cypher string, params map[string]any) (neo4j.ResultWithContext, error) {
	if t.tenantID == "" || t.runner == nil {
		return nil, errEmptyTenant
	}
	if _, ok := params[tenantParam]; ok {
		return nil, errTenantParamOverride
	}
	bound := maps.Clone(params)
	if bound == nil {
		bound = make(map[string]any, 1)
	}
	bound[tenantParam] = t.tenantID
	return t.runner.Run(ctx, cypher, bound)
}

// requireTenant refuses tenantID unless it is the runner's tenant, so a
// repository on a transaction bound to one tenant never acts for another.
func (t TenantRunner) requireTenant(tenantID string) error {
	if tenantID == "" || t.tenantID == "" {
		return errEmptyTenant
	}
	if tenantID != t.tenantID {
		return fmt.Errorf("%w: called for %q on a transaction of %q", errTenantMismatch, tenantID, t.tenantID)
	}
	return nil
}

// inReadTx runs body in one read transaction whose statements are bound to
// tenantID, and returns what body returns. body reads every record it needs
// before it returns, since the records go with the transaction, and builds
// its result afresh on every call: the driver runs it again when the
// transaction fails in a way a retry can fix. An empty tenant opens no
// session.
func inReadTx[T any](ctx context.Context, client Neo4jClient, tenantID string, body func(run TenantRunner) (T, error)) (T, error) {
	if tenantID == "" {
		var zero T
		return zero, errEmptyTenant
	}
	session := client.NewSession(ctx, neo4j.AccessModeRead)
	defer func() { _ = session.Close(ctx) }()
	return neo4j.ExecuteRead(ctx, session, func(tx neo4j.ManagedTransaction) (T, error) {
		run, err := NewTenantRunner(tenantID, tx)
		if err != nil {
			var zero T
			return zero, err
		}
		return body(run)
	})
}

// execCypher runs a statement that returns nothing the caller reads, and
// consumes its result so a failure the server reports only when the result is
// pulled is not lost.
func execCypher(ctx context.Context, run TenantRunner, cypher string, params map[string]any) error {
	res, err := run.Run(ctx, cypher, params)
	if err != nil {
		return err
	}
	if _, err := res.Consume(ctx); err != nil {
		return err
	}
	return nil
}

// singleBool runs a statement that returns one row and reads the boolean in
// column key of it.
func singleBool(ctx context.Context, run TenantRunner, cypher string, params map[string]any, key string) (bool, error) {
	res, err := run.Run(ctx, cypher, params)
	if err != nil {
		return false, err
	}
	rec, err := res.Single(ctx)
	if err != nil {
		return false, err
	}
	v, _ := rec.Get(key)
	b, ok := v.(bool)
	if !ok {
		return false, fmt.Errorf("neo4j: column %q is %T, want a boolean", key, v)
	}
	return b, nil
}
