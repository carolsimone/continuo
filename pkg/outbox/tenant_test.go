package outbox_test

import (
	"testing"

	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWrapTenant_RoundTrips(t *testing.T) {
	payload := []byte(`{"run_id":"r1","attempt":2}`)

	stored, err := outbox.WrapTenant("acme", payload)
	require.NoError(t, err)

	tenantID, got, err := outbox.UnwrapTenant(stored)
	require.NoError(t, err)
	assert.Equal(t, "acme", tenantID)
	assert.JSONEq(t, string(payload), string(got))
}

func TestWrapTenant_RefusesAnEmptyTenant(t *testing.T) {
	_, err := outbox.WrapTenant("", []byte(`{}`))

	require.ErrorIs(t, err, outbox.ErrNoTenant)
}

func TestWrapTenant_RefusesAPayloadThatIsNotJSON(t *testing.T) {
	_, err := outbox.WrapTenant("acme", []byte(`{not json`))

	require.Error(t, err)
}

func TestUnwrapTenant_RefusesARowWithoutATenant(t *testing.T) {
	for name, stored := range map[string]string{
		"a bare payload":      `{"run_id":"r1"}`,
		"an empty tenant":     `{"tenant_id":"","payload":{"run_id":"r1"}}`,
		"a missing payload":   `{"tenant_id":"acme"}`,
		"a null payload":      `{"tenant_id":"acme","payload":null}`,
		"not an object":       `"text"`,
		"not JSON":            `{`,
		"an empty stored row": ``,
	} {
		t.Run(name, func(t *testing.T) {
			_, _, err := outbox.UnwrapTenant([]byte(stored))

			require.ErrorIs(t, err, outbox.ErrNoTenant)
		})
	}
}
