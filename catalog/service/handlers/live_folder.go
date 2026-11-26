package handlers

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/carolsimone/continuo/catalog/domain/contractversion"
	domainModel "github.com/carolsimone/continuo/catalog/domain/model"
	"github.com/carolsimone/continuo/catalog/service/ports"
	"github.com/carolsimone/continuo/catalog/service/queries"
)

// ErrLiveFolderBusy means another rewrite holds the tenant's live-folder
// lease, so this one wrote nothing. The caller tries again later.
var ErrLiveFolderBusy = errors.New("another rewrite holds the live contracts folder")

const (
	// liveLeaseTTL is how long a rewrite's live-folder lease lasts unless it
	// is renewed. A rewrite that dies blocks the folder for at most this long.
	liveLeaseTTL = 2 * time.Minute
	// liveLeaseRenewEvery is how often a running rewrite renews its lease.
	liveLeaseRenewEvery = liveLeaseTTL / 3
	// liveRenewTimeout bounds one renewal of the lease. A renewal that has
	// not answered by then fails the rewrite and cancels its writes while the
	// lease, renewed at most liveLeaseRenewEvery earlier, still has at least
	// liveLeaseTTL - liveLeaseRenewEvery - liveRenewTimeout (50 s) to run, so
	// no write of the rewrite outlives its lease.
	liveRenewTimeout = 30 * time.Second
	// liveReleaseTimeout bounds the release of the lease after a rewrite,
	// which runs even when the rewrite's context has ended.
	liveReleaseTimeout = 10 * time.Second
	// liveWriteWorkers is how many writes of one rewrite run at once, so a
	// rewrite of every file of a large tenant ends well inside its message's
	// handler deadline.
	liveWriteWorkers = 16
)

// LiveFolder writes a tenant's live contracts folder: one published contract
// file per node of the promotion it is written for whose current contract is
// not an ephemeral model's, and the folder's index, whose final form is
// written last. The nodes of
// a promotion are those whose contract watermark is its seq: every bundle
// names every node of the release that has a contract (each dbt model, seed
// and snapshot and each Python node), so the set never waits for the
// topology swap, takes a node the release added at once and drops a node the
// release removed at once. A file's bytes depend only on the node's current
// contract version, the schema the node is declared in and the publishing
// settings, so a rewrite puts only the files whose version, contract hash or
// schema differs from the index, or every file when the index was written with
// other settings, and deletes the files of the nodes the index lists that are
// no longer published. The schema reaches the file as its servers entry's
// schema, which is the node's declared spelling, not the lower-case schema of
// its contract id; the index keeps it only for a file whose entry names a
// schema other than the id's (see indexedSchema). The declared schema of a
// node is the one the promotion's own topology artifact names, which a rewrite
// for a promotion reads (see WithDeclaredSchemas), so a node the contracts group
// created before the topology swap, and one whose schema changes only in case,
// get their final spelling at once, whichever of the groups runs first. A
// rewrite with no artifact, the one a settings change or a start-up refresh
// makes, names the schema the graph holds.
//
// Every rewrite of a tenant holds the tenant's live-folder lease, so two
// rewrites never interleave their writes, and reads the live contracts only
// once it holds the lease, so a later rewrite always writes a later state. A
// rewrite for a promotion older than the newest one whose contracts are
// recorded writes nothing: that newer promotion's own rewrite writes the
// folder, and the older set would put superseded files back.
type LiveFolder struct {
	contracts queries.ContractReader
	store     ports.LiveContractsStore
	renderer  ports.PublishedRenderer
	lease     ports.LiveFolderLease
	state     ports.LiveFolderState
	metrics   ports.ContractMetrics
	server    *contractversion.Server
	settings  string
	now       func() time.Time
	logger    *slog.Logger
	// ontologyBaseURL is the URL the published ontology links start with;
	// empty publishes them as stored.
	ontologyBaseURL string
	// leaseTTL, leaseRenewEvery and renewTimeout time the live-folder lease:
	// liveLeaseTTL, liveLeaseRenewEvery and liveRenewTimeout.
	leaseTTL        time.Duration
	leaseRenewEvery time.Duration
	renewTimeout    time.Duration
	// artifacts reads a promotion's topology artifact, for the schema each node
	// is declared in; nil leaves the schema to the graph.
	artifacts ports.TopologyArtifactReader
}

// LiveFolderOption sets an optional collaborator of a LiveFolder.
type LiveFolderOption func(*LiveFolder)

// WithDeclaredSchemas makes a rewrite for a promotion name, in each file, the
// schema its topology artifact declares the node in, in place of the schema the
// graph holds, which is empty or stale until the promotion's topology swap has
// run. A promotion's artifact that cannot be read as one (corrupt, or another
// tenant's or release's) is ignored, since the swap dead-letters it, and the
// graph's schemas are used; an artifact that is missing, or storage that fails,
// fails the rewrite, so the message is retried.
func WithDeclaredSchemas(artifacts ports.TopologyArtifactReader) LiveFolderOption {
	return func(l *LiveFolder) { l.artifacts = artifacts }
}

// NewLiveFolder creates a LiveFolder that publishes with server, the install's
// warehouse entry (nil, or a server with no host, publishes no servers entry),
// and ontologyBaseURL, the URL the files' ontology links start with ("" keeps
// them as stored).
func NewLiveFolder(
	contracts queries.ContractReader,
	store ports.LiveContractsStore,
	renderer ports.PublishedRenderer,
	lease ports.LiveFolderLease,
	state ports.LiveFolderState,
	metrics ports.ContractMetrics,
	server *contractversion.Server,
	ontologyBaseURL string,
	now func() time.Time,
	logger *slog.Logger,
	options ...LiveFolderOption,
) *LiveFolder {
	l := &LiveFolder{
		contracts: contracts,
		store:     store,
		renderer:  renderer,
		lease:     lease,
		state:     state,
		metrics:   metrics,
		server:    server,
		settings:  settingsFingerprint(server, ontologyBaseURL),
		now:       now,
		logger:    logger,

		ontologyBaseURL: ontologyBaseURL,

		leaseTTL:        liveLeaseTTL,
		leaseRenewEvery: liveLeaseRenewEvery,
		renewTimeout:    liveRenewTimeout,
	}
	for _, option := range options {
		option(l)
	}
	return l
}

// declaredSchemas is the schema each node of promotion in is declared in, by
// unique id, as the promotion's topology artifact names it, or nil when the
// folder reads none, the promotion names none, or the artifact is not the
// promoted tenant's and release's (see WithDeclaredSchemas).
func (l *LiveFolder) declaredSchemas(ctx context.Context, in domainModel.PromoteReleaseInput) (map[string]string, error) {
	if l.artifacts == nil || in.TopologyURI == "" {
		return nil, nil
	}
	doc, err := l.artifacts.Load(ctx, in.TopologyURI, in.TopologySHA256)
	if err != nil {
		if errors.Is(err, ports.ErrTopologyArtifactCorrupt) {
			l.logger.Warn("the promotion's topology artifact is unreadable, so the live contracts name the schemas the graph holds",
				"tenant_id", in.TenantID, "release_id", in.ReleaseID, "uri", in.TopologyURI, "error", err)
			return nil, nil
		}
		return nil, fmt.Errorf("load topology artifact %s: %w", in.TopologyURI, err)
	}
	if doc.TenantID != in.TenantID || doc.ReleaseID != in.ReleaseID {
		l.logger.Warn("the promotion's topology artifact belongs to another tenant or release, so the live contracts name the schemas the graph holds",
			"tenant_id", in.TenantID, "release_id", in.ReleaseID, "artifact_tenant_id", doc.TenantID,
			"artifact_release_id", doc.ReleaseID, "uri", in.TopologyURI)
		return nil, nil
	}
	schemas := make(map[string]string, len(doc.Nodes))
	for _, n := range doc.Nodes {
		if n.SchemaName != "" {
			schemas[n.UniqueID] = n.SchemaName
		}
	}
	return schemas, nil
}

// settingsFingerprint is the hex SHA-256 fingerprint of the publishing
// settings: the server (nil fingerprints as a server with no host) and, when
// it is set, the ontology base URL. A change of either rewrites every file.
func settingsFingerprint(server *contractversion.Server, ontologyBaseURL string) string {
	return contractversion.PublishingFingerprint(server, ontologyBaseURL, func(b []byte) string {
		sum := sha256.Sum256(b)
		return hex.EncodeToString(sum[:])
	})
}

// Rewrite brings the tenant's folder up to date after the promotion in. It
// does nothing when in.PromotionSeq is at or below the folder's watermark,
// when the index was already written for this promotion or a later one with
// the same settings (an earlier delivery finished the rewrite but not its
// transaction B), or when a newer promotion's contracts are already recorded.
// Otherwise, holding the tenant's live-folder lease, it puts each changed file
// of the promotion's nodes, deletes each file no longer published and puts
// the index. A lease another rewrite holds is ErrLiveFolderBusy; a write that
// fails ends the rewrite before the final index, so the index still lists
// every file the folder may hold, and the message's redelivery finishes the
// work.
//
// It returns the fingerprint of the settings the folder's files carry once it
// returns: this folder's when it wrote the index, or found the index already
// written for this promotion with them; "" when it wrote nothing and cannot
// tell, so transaction B leaves the stored fingerprint as it is.
func (l *LiveFolder) Rewrite(ctx context.Context, in domainModel.PromoteReleaseInput) (string, error) {
	if done, err := l.atWatermark(ctx, in); err != nil || done {
		return "", err
	}
	settings := ""
	err := l.underLease(ctx, in.TenantID, func(ctx context.Context) error {
		// Read again under the lease: a rewrite that held it meanwhile may
		// have finished this promotion or a later one.
		if done, err := l.atWatermark(ctx, in); err != nil || done {
			return err
		}
		prev, found, err := l.readIndex(ctx, in.TenantID)
		if err != nil {
			return err
		}
		if found && prev.SettingsSHA256 == l.settings && prev.PromotionSeq >= in.PromotionSeq {
			l.logger.Info("live contracts folder already written for this promotion; nothing to write",
				"tenant_id", in.TenantID, "promotion_seq", in.PromotionSeq, "index_promotion_seq", prev.PromotionSeq)
			settings = l.settings
			return nil
		}
		if newer, err := l.superseded(ctx, in.TenantID, in.PromotionSeq); err != nil || newer {
			return err
		}
		declared, err := l.declaredSchemas(ctx, in)
		if err != nil {
			return err
		}
		if err := l.write(ctx, in.TenantID, prev, found, false, in.PromotionSeq, in.ReleaseID, declared); err != nil {
			return err
		}
		settings = l.settings
		return nil
	})
	if err != nil {
		return "", err
	}
	return settings, nil
}

// readIndex reads the tenant's index. An index the store cannot read fails
// every rewrite until someone deletes it, so it is logged as an error that
// names the fix.
func (l *LiveFolder) readIndex(ctx context.Context, tenantID string) (ports.LiveIndex, bool, error) {
	prev, found, err := l.store.ReadIndex(ctx, tenantID)
	if errors.Is(err, ports.ErrLiveIndexUnreadable) {
		l.logger.Error("live contracts index unreadable; no rewrite can go on until it is fixed:"+
			" delete the index the error names, and the next rewrite puts every file again",
			"tenant_id", tenantID, "error", err)
	}
	if err != nil {
		return ports.LiveIndex{}, false, fmt.Errorf("read the live index: %w", err)
	}
	return prev, found, nil
}

// superseded reports whether a promotion newer than seq has its contracts
// recorded, so a rewrite for seq would publish a superseded set.
func (l *LiveFolder) superseded(ctx context.Context, tenantID string, seq int64) (bool, error) {
	latest, err := l.contracts.LatestPromotionSeq(ctx, tenantID)
	if err != nil {
		return false, fmt.Errorf("read the newest contract promotion: %w", err)
	}
	if latest > seq {
		l.logger.Info("a newer promotion's contracts are recorded; its rewrite writes the live folder",
			"tenant_id", tenantID, "promotion_seq", seq, "newest_promotion_seq", latest)
		return true, nil
	}
	return false, nil
}

// atWatermark reports whether the folder's watermark is at or above the
// promotion's seq.
func (l *LiveFolder) atWatermark(ctx context.Context, in domainModel.PromoteReleaseInput) (bool, error) {
	mark, err := l.state.Read(ctx, in.TenantID)
	if err != nil {
		return false, fmt.Errorf("read the live folder's watermark: %w", err)
	}
	if in.PromotionSeq <= mark.PromotionSeq {
		l.logger.Info("promotion at or below the live folder's watermark; nothing to write",
			"tenant_id", in.TenantID, "promotion_seq", in.PromotionSeq, "watermark", mark.PromotionSeq)
		return true, nil
	}
	return false, nil
}

// RewriteAll rewrites the folder for the promotion its watermark names: it
// puts every published file of that promotion's nodes, deletes each file the
// index lists that is no longer published, puts the index, which names the
// watermark's promotion and release, and stores the settings fingerprint. A
// folder with no watermark has nothing to rewrite. A newer promotion whose
// contracts are recorded leaves the folder to that promotion's own rewrite,
// which puts every file written with other settings.
func (l *LiveFolder) RewriteAll(ctx context.Context, tenantID string) error {
	return l.underLease(ctx, tenantID, func(ctx context.Context) error {
		mark, err := l.state.Read(ctx, tenantID)
		if err != nil {
			return fmt.Errorf("read the live folder's watermark: %w", err)
		}
		if mark.PromotionSeq == 0 {
			return nil
		}
		if newer, err := l.superseded(ctx, tenantID, mark.PromotionSeq); err != nil || newer {
			return err
		}
		prev, found, err := l.readIndex(ctx, tenantID)
		if err != nil {
			return err
		}
		if err := l.write(ctx, tenantID, prev, found, true, mark.PromotionSeq, mark.ReleaseID, nil); err != nil {
			return err
		}
		if err := l.state.RecordSettings(ctx, tenantID, l.settings); err != nil {
			return fmt.Errorf("record the live folder's settings: %w", err)
		}
		return nil
	})
}

// RefreshAtStart rewrites every file of each tenant whose folder was written
// with other publishing settings than this folder's, and leaves alone a
// tenant whose folder is current or was never written. A pass that fails for
// any tenant is logged, and the next pass, retry later, tries again every
// tenant still behind, until one pass succeeds or ctx ends.
func (l *LiveFolder) RefreshAtStart(ctx context.Context, tenants ports.GraphTenants, retry time.Duration) error {
	for {
		err := l.refreshPass(ctx, tenants)
		if err == nil {
			return nil
		}
		l.logger.Error("live contracts folder start-up rewrite failed; trying again",
			"error", err, "retry_in", retry.String())
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(retry):
		}
	}
}

// refreshPass is one pass of RefreshAtStart over every tenant.
func (l *LiveFolder) refreshPass(ctx context.Context, tenants ports.GraphTenants) error {
	ids, err := tenants.Tenants(ctx)
	if err != nil {
		return fmt.Errorf("list the tenants: %w", err)
	}
	var errs []error
	for _, tenantID := range ids {
		mark, err := l.state.Read(ctx, tenantID)
		if err != nil {
			errs = append(errs, fmt.Errorf("tenant %q: read the live folder's state: %w", tenantID, err))
			continue
		}
		if mark.SettingsSHA256 == "" || mark.SettingsSHA256 == l.settings {
			continue
		}
		l.logger.Info("publishing settings changed; rewriting every live contract file",
			"tenant_id", tenantID, "stored_settings_sha256", mark.SettingsSHA256, "settings_sha256", l.settings)
		if err := l.RewriteAll(ctx, tenantID); err != nil {
			errs = append(errs, fmt.Errorf("tenant %q: %w", tenantID, err))
		}
	}
	return errors.Join(errs...)
}

// underLease runs fn while holding the tenant's live-folder lease, renewing
// it every leaseRenewEvery, each renewal bounded by renewTimeout. A renewal
// that finds the lease lost, fails or runs out of time cancels fn's context
// with that error, which is returned. The lease is released when
// fn returns, even after ctx ended; a release that fails leaves the lease to
// lapse.
func (l *LiveFolder) underLease(ctx context.Context, tenantID string, fn func(ctx context.Context) error) error {
	holder := "live-folder-" + uuid.NewString()
	acquired, err := l.lease.Acquire(ctx, tenantID, holder, l.leaseTTL)
	if err != nil {
		return fmt.Errorf("take the live-folder lease: %w", err)
	}
	if !acquired {
		return fmt.Errorf("tenant %q: %w", tenantID, ErrLiveFolderBusy)
	}
	defer func() {
		releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), liveReleaseTimeout)
		defer cancel()
		if err := l.lease.Release(releaseCtx, tenantID, holder); err != nil {
			l.logger.Warn("live-folder lease not released; it lapses on its own",
				"tenant_id", tenantID, "holder", holder, "error", err)
		}
	}()

	work, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	stop := keepAlive(work, l.leaseRenewEvery, l.renewTimeout, func(ctx context.Context) error {
		return l.lease.Renew(ctx, tenantID, holder, l.leaseTTL)
	}, func(err error) {
		cancel(fmt.Errorf("renew the live-folder lease: %w", err))
	})
	err = fn(work)
	stop()
	if err != nil {
		if cause := context.Cause(work); cause != nil {
			return cause
		}
		return err
	}
	return nil
}

// write puts the files of promotionSeq's live contracts that must be written
// (every one when all is set, or when prev was not found or was written with
// other settings), deletes the files prev lists that are no longer published,
// then puts the index naming promotionSeq and releaseID. declared, when it is
// not nil, names the schema each node is declared in, by unique id, and takes
// the place of the schema the graph holds for it.
//
// Before its writes it puts an interim index: prev with every file about to be
// put or deleted listed at version 0 and an empty hash, which match no
// contract. A rewrite that stops after a put or a delete leaves that file
// listed at version 0, so the next rewrite, whichever promotion and settings
// it is for, deletes the file if its node is not published and puts it again
// if it is: a file a failed rewrite added, overwrote with another version or
// other settings, or deleted, is never left as the index does not say.
func (l *LiveFolder) write(
	ctx context.Context,
	tenantID string,
	prev ports.LiveIndex,
	found, all bool,
	promotionSeq int64,
	releaseID string,
	declared map[string]string,
) error {
	live, err := l.contracts.Live(ctx, tenantID, promotionSeq)
	if err != nil {
		return fmt.Errorf("read the live contracts: %w", err)
	}
	listed := make(map[string]ports.LiveIndexFile, len(prev.Files))
	for _, f := range prev.Files {
		listed[f.UniqueID] = f
	}
	every := all || !found || prev.SettingsSHA256 != l.settings

	files := make([]ports.LiveIndexFile, 0, len(live))
	published := make(map[string]bool, len(live))
	var puts []contractversion.LiveContract
	for _, c := range live {
		if contractversion.IsEphemeral(c.Document) {
			continue
		}
		if schema, ok := declared[c.UniqueID]; ok {
			c.PhysicalSchema = schema
		}
		file := ports.LiveIndexFile{
			UniqueID: c.UniqueID, Version: c.Version, ContractHash: c.ContractHash, PhysicalSchema: l.indexedSchema(c),
		}
		files = append(files, file)
		published[c.UniqueID] = true
		if old, ok := listed[c.UniqueID]; every || !ok || old != file {
			puts = append(puts, c)
		}
	}
	var deletes []string
	for _, f := range prev.Files {
		if !published[f.UniqueID] {
			deletes = append(deletes, f.UniqueID)
		}
	}
	slices.SortFunc(files, func(a, b ports.LiveIndexFile) int { return cmp.Compare(a.UniqueID, b.UniqueID) })
	slices.SortFunc(puts, func(a, b contractversion.LiveContract) int { return cmp.Compare(a.UniqueID, b.UniqueID) })
	slices.Sort(deletes)

	if err := l.putInterimIndex(ctx, tenantID, prev, found, puts, deletes, promotionSeq, releaseID); err != nil {
		return err
	}
	if err := l.each(ctx, len(puts), ports.LiveWritePut, func(ctx context.Context, i int) error {
		c := puts[i]
		body, err := l.renderer.Render(contractversion.Published(c.Document, c.Version, l.server, c.PhysicalSchema, c.Provenance, l.ontologyBaseURL))
		if err != nil {
			return fmt.Errorf("render the live contract of %s: %w", c.UniqueID, err)
		}
		return l.store.Put(ctx, tenantID, c.UniqueID, body)
	}); err != nil {
		return err
	}
	if err := l.each(ctx, len(deletes), ports.LiveWriteDelete, func(ctx context.Context, i int) error {
		return l.store.Delete(ctx, tenantID, deletes[i])
	}); err != nil {
		return err
	}
	idx := ports.LiveIndex{
		SchemaVersion:  ports.LiveIndexSchemaVersion,
		TenantID:       tenantID,
		PromotionSeq:   promotionSeq,
		ReleaseID:      releaseID,
		WrittenAt:      l.now().UTC(),
		SettingsSHA256: l.settings,
		Files:          files,
	}
	if err := l.store.PutIndex(ctx, tenantID, idx); err != nil {
		l.metrics.LiveWrite(ports.LiveWriteIndex, ports.LiveWriteFailed)
		return fmt.Errorf("put the live index: %w", err)
	}
	l.metrics.LiveWrite(ports.LiveWriteIndex, ports.LiveWriteOK)
	l.logger.Info("Live contracts folder rewritten",
		"tenant_id", tenantID, "promotion_seq", promotionSeq, "release_id", releaseID,
		"files", len(files), "puts", len(puts), "deletes", len(deletes))
	return nil
}

// indexedSchema is the PhysicalSchema the index records for c's file: the
// schema its servers entry names when that is not the schema part of its
// contract id, and "" when it is, or when the file has no servers entry,
// whatever the node's declared schema, as the settings fingerprint ignores the
// settings a file does not show. An index entry that records no schema stands
// for a file whose entry names the id's schema, so it is current for a node
// whose declared schema agrees with its id's or is not known yet, and stale for
// a node declared in another spelling. A file is put again when the schema its
// entry names changes, a change of case alone included, and for no other
// reason.
func (l *LiveFolder) indexedSchema(c contractversion.LiveContract) string {
	if l.server == nil || l.server.Host == "" {
		return ""
	}
	schema := contractversion.ServerSchema(c.Document, c.PhysicalSchema)
	if schema == contractversion.ServerSchema(c.Document, "") {
		return ""
	}
	return schema
}

// putInterimIndex puts the interim index write describes when puts or
// deletes is not empty; otherwise it writes nothing. The interim index keeps prev's
// promotion, release and settings, so a rewrite reading it still sees the
// folder's last finished rewrite. With no prev it names the promotion and
// release being written and no settings, which no rewrite takes as finished.
func (l *LiveFolder) putInterimIndex(
	ctx context.Context,
	tenantID string,
	prev ports.LiveIndex,
	found bool,
	puts []contractversion.LiveContract,
	deletes []string,
	promotionSeq int64,
	releaseID string,
) error {
	if len(puts) == 0 && len(deletes) == 0 {
		return nil
	}
	pending := make(map[string]bool, len(puts)+len(deletes))
	for _, c := range puts {
		pending[c.UniqueID] = true
	}
	for _, id := range deletes {
		pending[id] = true
	}
	files := make([]ports.LiveIndexFile, 0, len(prev.Files)+len(puts))
	for _, f := range prev.Files {
		if !pending[f.UniqueID] {
			files = append(files, f)
		}
	}
	for id := range pending {
		files = append(files, ports.LiveIndexFile{UniqueID: id})
	}
	slices.SortFunc(files, func(a, b ports.LiveIndexFile) int { return cmp.Compare(a.UniqueID, b.UniqueID) })
	interim := prev
	if !found {
		interim = ports.LiveIndex{
			SchemaVersion: ports.LiveIndexSchemaVersion, TenantID: tenantID,
			PromotionSeq: promotionSeq, ReleaseID: releaseID,
		}
	}
	interim.WrittenAt = l.now().UTC()
	interim.Files = files
	if err := l.store.PutIndex(ctx, tenantID, interim); err != nil {
		l.metrics.LiveWrite(ports.LiveWriteIndex, ports.LiveWriteFailed)
		return fmt.Errorf("put the interim live index: %w", err)
	}
	l.metrics.LiveWrite(ports.LiveWriteIndex, ports.LiveWriteOK)
	return nil
}

// each runs do for 0..n-1, at most liveWriteWorkers at once, counting each
// call's outcome under op. The first failure stops the calls not yet started
// and is returned; so is ctx's cause when ctx ends first.
func (l *LiveFolder) each(ctx context.Context, n int, op string, do func(ctx context.Context, i int) error) error {
	work, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	slots := make(chan struct{}, liveWriteWorkers)
	var (
		wg    sync.WaitGroup
		once  sync.Once
		first error
	)
	for i := range n {
		select {
		case slots <- struct{}{}:
		case <-work.Done():
		}
		if work.Err() != nil {
			break
		}
		wg.Go(func() {
			defer func() { <-slots }()
			if err := do(work, i); err != nil {
				l.metrics.LiveWrite(op, ports.LiveWriteFailed)
				once.Do(func() { first = err; cancel(err) })
				return
			}
			l.metrics.LiveWrite(op, ports.LiveWriteOK)
		})
	}
	wg.Wait()
	if first != nil {
		return first
	}
	return context.Cause(ctx)
}

// keepAlive calls renew every interval until stop is called or ctx ends, each
// call under a context that ends after timeout, handing each error renew
// returns (a call cut short by timeout among them) to onFail. stop waits for a
// renewal in flight, so nothing renews after it returns.
func keepAlive(ctx context.Context, every, timeout time.Duration, renew func(context.Context) error, onFail func(error)) (stop func()) {
	done := make(chan struct{})
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		ticker := time.NewTicker(every)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				call, cancel := context.WithTimeout(ctx, timeout)
				err := renew(call)
				cancel()
				if err != nil {
					onFail(err)
				}
			}
		}
	}()
	var once sync.Once
	return func() {
		once.Do(func() { close(done) })
		<-finished
	}
}
