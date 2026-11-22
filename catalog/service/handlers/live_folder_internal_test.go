package handlers

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/catalog/domain/contractversion"
	domainModel "github.com/carolsimone/continuo/catalog/domain/model"
	"github.com/carolsimone/continuo/catalog/service/ports"
)

// A renewal that never answers is cut short by its timeout and handed to
// onFail, and the next renewal still runs.
func TestKeepAlive_AStalledRenewalTimesOut(t *testing.T) {
	var (
		mu       sync.Mutex
		failures []error
	)
	stop := keepAlive(context.Background(), 5*time.Millisecond, 10*time.Millisecond,
		func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		},
		func(err error) {
			mu.Lock()
			defer mu.Unlock()
			failures = append(failures, err)
		})
	time.Sleep(80 * time.Millisecond)
	stop()

	mu.Lock()
	defer mu.Unlock()
	require.GreaterOrEqual(t, len(failures), 2)
	for _, err := range failures {
		assert.ErrorIs(t, err, context.DeadlineExceeded)
	}
}

// stallingLease grants every Acquire and blocks every Renew until its
// context ends.
type stallingLease struct{}

func (stallingLease) Acquire(context.Context, string, string, time.Duration) (bool, error) {
	return true, nil
}

func (stallingLease) Renew(ctx context.Context, _, _ string, _ time.Duration) error {
	<-ctx.Done()
	return ctx.Err()
}

func (stallingLease) Release(context.Context, string, string) error { return nil }

func (stallingLease) Read(context.Context, string) (ports.LiveFolderMark, error) {
	return ports.LiveFolderMark{}, nil
}

func (stallingLease) RecordSettings(context.Context, string, string) error { return nil }

// slowReader answers Live once its context ends, as a read stuck on Neo4j
// that the rewrite's cancellation stops.
type slowReader struct{}

func (slowReader) Current(context.Context, string, string) (contractversion.VersionView, error) {
	return contractversion.VersionView{}, contractversion.ErrNotFound
}

func (slowReader) History(context.Context, string, string, int) ([]contractversion.VersionView, error) {
	return nil, nil
}

func (slowReader) ByVersion(context.Context, string, string, int64) (contractversion.VersionView, error) {
	return contractversion.VersionView{}, contractversion.ErrNotFound
}

func (slowReader) Live(ctx context.Context, _ string, _ int64) ([]contractversion.LiveContract, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (slowReader) LatestPromotionSeq(context.Context, string) (int64, error) { return 0, nil }

// indexStore records whether any index was put.
type indexStore struct {
	mu      sync.Mutex
	indexes int
}

func (*indexStore) Put(context.Context, string, string, []byte) error { return nil }
func (*indexStore) Delete(context.Context, string, string) error      { return nil }
func (*indexStore) ReadIndex(context.Context, string) (ports.LiveIndex, bool, error) {
	return ports.LiveIndex{}, false, nil
}

func (s *indexStore) PutIndex(context.Context, string, ports.LiveIndex) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.indexes++
	return nil
}

type silentMetrics struct{}

func (silentMetrics) VersionsWritten(int)               {}
func (silentMetrics) LiveWrite(string, string)          {}
func (silentMetrics) ObserveStep(string, time.Duration) {}

type nopRenderer struct{}

func (nopRenderer) Render(map[string]any) ([]byte, error) { return nil, nil }

// A lease renewal that stalls fails the rewrite at its timeout, cancelling the
// work still running, instead of letting the lease lapse under it; no index is
// written.
func TestLiveRewrite_AStalledLeaseRenewalFailsTheRewrite(t *testing.T) {
	store := &indexStore{}
	folder := NewLiveFolder(slowReader{}, store, nopRenderer{}, stallingLease{}, stallingLease{}, silentMetrics{},
		nil, "", time.Now, slog.New(slog.NewTextHandler(io.Discard, nil)))
	folder.leaseRenewEvery = 10 * time.Millisecond
	folder.renewTimeout = 20 * time.Millisecond

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	settings, err := folder.Rewrite(ctx, domainModel.PromoteReleaseInput{TenantID: "default", ReleaseID: "rel-3", PromotionSeq: 3})

	require.Error(t, err)
	assert.ErrorIs(t, err, context.DeadlineExceeded, "the renewal's own timeout, not the caller's")
	assert.Contains(t, err.Error(), "renew the live-folder lease")
	assert.NoError(t, ctx.Err(), "the caller's context is still live")
	assert.Empty(t, settings)
	assert.Zero(t, store.indexes)
	assert.False(t, errors.Is(err, context.Canceled))
}

// An install that sets no ontology base URL keeps the settings fingerprint its
// live folder was written with, so an upgrade rewrites no file. The values are
// the SHA-256 of the canonical text of the server alone: "server=none\n" for no
// server and the four server lines for a warehouse.
func TestSettingsFingerprint_AnEmptyBaseURLKeepsTheOldFingerprint(t *testing.T) {
	warehouse := &contractversion.Server{Type: "postgres", Host: "warehouse.internal", Port: 5432, Database: "continuo_dbt"}

	assert.Equal(t, "ded41b553a6e8f798c7e35b01b95fca3c4457b8d5c5294a66dcdb878dea352a5", settingsFingerprint(nil, ""),
		"no server and no base URL")
	assert.Equal(t, "ded41b553a6e8f798c7e35b01b95fca3c4457b8d5c5294a66dcdb878dea352a5",
		settingsFingerprint(&contractversion.Server{Type: "postgres", Port: 5432}, ""), "a server with no host is no server")
	assert.Equal(t, "711a4c7b2e1195304e7a3d9a7bf95337c9b99e5ef6dbec8fc4d385fd60e6e723", settingsFingerprint(warehouse, ""),
		"a warehouse server and no base URL")
}

// A base URL is part of what a file shows, so setting it, or changing it, gives
// the folder a new fingerprint, with or without a server.
func TestSettingsFingerprint_ABaseURLChangesTheFingerprint(t *testing.T) {
	warehouse := &contractversion.Server{Type: "postgres", Host: "warehouse.internal", Port: 5432, Database: "continuo_dbt"}
	for name, server := range map[string]*contractversion.Server{"no server": nil, "a warehouse": warehouse} {
		none := settingsFingerprint(server, "")
		a := settingsFingerprint(server, "https://ontology.example/things")
		b := settingsFingerprint(server, "https://ontology.example/other")
		assert.NotEqual(t, none, a, name)
		assert.NotEqual(t, a, b, name)
		assert.Equal(t, a, settingsFingerprint(server, "https://ontology.example/things"), name)
	}
}
