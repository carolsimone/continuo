package outbox

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
)

// ErrNoTenant reports an outbox row, or a payload about to be stored as one,
// that names no tenant.
var ErrNoTenant = errors.New("outbox row has no tenant")

// tenantRow is how a service stores a row that belongs to one tenant: the
// tenant beside the payload it publishes. The outbox columns are the same for
// every service, so the tenant travels inside the stored payload.
type tenantRow struct {
	TenantID string          `json:"tenant_id"`
	Payload  json.RawMessage `json:"payload"`
}

// WrapTenant returns the stored payload of a row of tenantID: {"tenant_id":…,
// "payload":…}. It refuses an empty tenant with ErrNoTenant and a payload that
// is not JSON.
func WrapTenant(tenantID string, payload []byte) ([]byte, error) {
	if tenantID == "" {
		return nil, ErrNoTenant
	}
	stored, err := json.Marshal(tenantRow{TenantID: tenantID, Payload: payload})
	if err != nil {
		return nil, fmt.Errorf("wrap the payload of tenant %q: %w", tenantID, err)
	}
	return stored, nil
}

// UnwrapTenant returns the tenant and the payload a row stored with WrapTenant.
// A stored payload that is not so wrapped, or names an empty tenant or no
// payload, returns an error wrapping ErrNoTenant.
func UnwrapTenant(stored []byte) (tenantID string, payload []byte, err error) {
	var row tenantRow
	if err := json.Unmarshal(stored, &row); err != nil {
		return "", nil, fmt.Errorf("%w: stored payload is not a tenant row: %v", ErrNoTenant, err)
	}
	if row.TenantID == "" {
		return "", nil, fmt.Errorf("%w: stored payload names no tenant", ErrNoTenant)
	}
	if len(row.Payload) == 0 || bytes.Equal(row.Payload, []byte("null")) {
		return "", nil, fmt.Errorf("%w: stored payload of tenant %q carries no payload", ErrNoTenant, row.TenantID)
	}
	return row.TenantID, row.Payload, nil
}
