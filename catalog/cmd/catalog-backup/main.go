// catalog-backup takes, lists and restores backups of a tenant's catalog graph.
// An operator runs it in catalog's container (`kubectl exec deploy/catalog --
// catalog-backup ...`).
//
//	catalog-backup take [--tenant <tenant>]
//	catalog-backup list [--tenant <tenant>]
//	catalog-backup restore <object key> --yes
//
// take stores a backup of each tenant's graph at once (of every tenant the
// catalog holds, or of the one --tenant names) and prints one JSON object per
// stored backup on its own line: {"object_key","bytes","nodes",
// "relationships","duration_seconds"}, the last being the seconds the tenant's
// writes were paused. list prints a JSON array of the stored backups, newest
// first: [{"key","size","last_modified"}]; without --tenant it covers only
// the tenants the catalog's graph holds. restore returns the graph of the
// tenant its object key names to that backup and prints {"object_key",
// "tenant_id","nodes","relationships"}; it deletes the tenant's graph before
// it loads the backup, so it refuses to run without --yes.
//
// stdout carries only that JSON; diagnostics go to stderr through log/slog.
// Exit codes: 0 ok, 1 error (a restore that fails after it wiped the graph
// leaves it partial: run the same restore again), 2 usage (an unknown
// command or flag, restore without --yes or with a key that is not a
// backup key), 3 skipped (another backup or a restore holds a tenant's lease).
// A backup of several tenants goes on past a tenant that fails or is skipped
// and prints every backup it stored; its exit code is 1 when any tenant
// failed, else 3 when any was skipped.
//
// It reads NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD and the S3 settings
// (S3_ENDPOINT_URL, S3_BUCKET, AWS_DEFAULT_REGION, and the keys unless an IAM
// role supplies them) catalog reads, and CATALOG_BACKUP_KEEP, the number of
// backups of a tenant a backup keeps. It needs nothing else of catalog's
// configuration. SIGINT and SIGTERM cancel the work in flight: a backup
// aborts its upload and releases the tenant's lease.
package main

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"slices"
	"strings"
	"syscall"
	"time"

	metricsinfra "github.com/carolsimone/continuo/catalog/adapters/metrics"
	neo4jinfra "github.com/carolsimone/continuo/catalog/adapters/neo4j"
	s3infra "github.com/carolsimone/continuo/catalog/adapters/s3"
	"github.com/carolsimone/continuo/catalog/config"
	"github.com/carolsimone/continuo/catalog/service/backup"
	"github.com/carolsimone/continuo/catalog/service/ports"
	pkgconfig "github.com/carolsimone/continuo/pkg/config"
	"github.com/carolsimone/continuo/pkg/objectstore"
)

const (
	exitOK      = 0
	exitError   = 1
	exitUsage   = 2
	exitSkipped = 3
)

const usageText = `catalog-backup takes, lists and restores backups of a tenant's catalog graph.

Usage:
  catalog-backup take [--tenant <tenant>]
      Take a backup of every tenant's graph (of one tenant with --tenant).
      Prints one JSON object per stored backup:
      {"object_key","bytes","nodes","relationships","duration_seconds"}.
  catalog-backup list [--tenant <tenant>]
      Print the stored backups, newest first, as a JSON array of
      {"key","size","last_modified"}. Without --tenant it covers only the
      tenants the catalog's graph holds: after a loss of the graph, name the
      tenant to find its backups.
  catalog-backup restore <object key> --yes
      Return the graph of the tenant the key names to that backup. This
      deletes the tenant's graph first, so --yes is required. Prints
      {"object_key","tenant_id","nodes","relationships"}.

An object key is tenants/<tenant>/backups/catalog/<UTC time>.jsonl.gz, as
list prints it. stdout carries only JSON; diagnostics go to stderr.
Exit codes: 0 ok, 1 error, 2 usage, 3 skipped (a lease is held).
`

// taker stores the backups of the tenants it is given, one after the other.
type taker interface {
	TakeEach(ctx context.Context, tenants []string, each func(tenantID string, res backup.Result, err error)) (notReached []string)
}

// restorer returns a tenant's graph to the backup stored at a key.
type restorer interface {
	Restore(ctx context.Context, key string) (backup.RestoreResult, error)
}

// backupLister lists a tenant's stored backups.
type backupLister interface {
	List(ctx context.Context, tenantID string) ([]ports.BackupObject, error)
}

// deps is what the commands run on.
type deps struct {
	taker    taker
	restorer restorer
	store    backupLister
	tenants  ports.GraphTenants
}

// connectFunc opens the dependencies, logging to logger, and returns a
// function that releases them.
type connectFunc func(ctx context.Context, logger *slog.Logger) (deps, func(), error)

// options are the parsed arguments of one command.
type options struct {
	// tenant is the --tenant value; tenantSet tells an absent flag from an
	// empty one.
	tenant    string
	tenantSet bool
	// key and keyTenant are restore's object key and the tenant it names.
	key       string
	keyTenant string
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	// A second signal ends the process, which the first one asked to stop.
	go func() {
		<-ctx.Done()
		stop()
	}()
	code := run(ctx, os.Args[1:], os.Stdout, os.Stderr, s3infra.BackupKeys{}, connect)
	stop()
	os.Exit(code)
}

// run executes one command line and returns the exit code. Arguments are
// checked with keys before anything is connected to, so a usage error needs
// no Neo4j or object store.
func run(ctx context.Context, args []string, stdout, stderr io.Writer, keys ports.BackupKeys, connect connectFunc) int {
	logger := slog.New(slog.NewTextHandler(stderr, nil))

	if len(args) == 0 {
		return usageError(logger, stderr, errors.New("no command given"))
	}
	command, rest := args[0], args[1:]
	switch command {
	case "-h", "--help", "help":
		_, _ = io.WriteString(stderr, usageText)
		return exitOK
	case "take", "list", "restore":
	default:
		return usageError(logger, stderr, fmt.Errorf("unknown command %q", command))
	}

	opts, err := parseOptions(command, rest, keys)
	if errors.Is(err, flag.ErrHelp) {
		_, _ = io.WriteString(stderr, usageText)
		return exitOK
	}
	if err != nil {
		return usageError(logger, stderr, err)
	}

	d, closeFn, err := connect(ctx, logger)
	if err != nil {
		logger.Error("connect", "error", err)
		return exitError
	}
	defer closeFn()

	switch command {
	case "take":
		return runTake(ctx, logger, stdout, d, opts)
	case "list":
		return runList(ctx, logger, stdout, d, opts)
	default:
		return runRestore(ctx, logger, stdout, d, opts)
	}
}

func usageError(logger *slog.Logger, stderr io.Writer, err error) int {
	logger.Error("usage", "error", err)
	_, _ = io.WriteString(stderr, usageText)
	return exitUsage
}

// parseOptions parses the arguments of command, checking a tenant and
// locating a key with keys. flag.ErrHelp means -h was given; any other error
// is a usage error.
func parseOptions(command string, args []string, keys ports.BackupKeys) (options, error) {
	fs := flag.NewFlagSet("catalog-backup "+command, flag.ContinueOnError)
	// The flag package would print each error and the flag defaults itself;
	// run reports the error and the usage instead.
	fs.SetOutput(io.Discard)
	fs.Usage = func() {}
	var o options
	var yes bool
	if command == "restore" {
		fs.BoolVar(&yes, "yes", false, "confirm that the tenant's graph is deleted before the backup is loaded")
	} else {
		fs.StringVar(&o.tenant, "tenant", "", "the tenant to act on; every tenant when omitted")
	}
	positional, err := parseInterspersed(fs, args)
	if err != nil {
		return options{}, err
	}
	fs.Visit(func(f *flag.Flag) { o.tenantSet = o.tenantSet || f.Name == "tenant" })

	if command != "restore" {
		if len(positional) > 0 {
			return options{}, fmt.Errorf("unexpected arguments: %s", strings.Join(positional, " "))
		}
		if o.tenantSet {
			if err := keys.CheckTenant(o.tenant); err != nil {
				return options{}, fmt.Errorf("--tenant %q cannot name a tenant: %w", o.tenant, err)
			}
		}
		return o, nil
	}

	if len(positional) != 1 {
		return options{}, errors.New("restore takes exactly one object key")
	}
	o.key = positional[0]
	if o.keyTenant, _, err = keys.Locate(o.key); err != nil {
		return options{}, fmt.Errorf("%q is not the object key of a catalog backup: a key looks like "+
			"tenants/<tenant>/backups/catalog/<UTC time>.jsonl.gz, and `catalog-backup list` prints them", o.key)
	}
	if !yes {
		return options{}, fmt.Errorf("restore deletes the graph of tenant %s and loads the backup %s instead; "+
			"nothing was changed: run it again with --yes to restore", o.keyTenant, o.key)
	}
	return o, nil
}

// parseInterspersed parses args with fs and returns the positional arguments,
// whether they come before or after the flags. A "--" ends the flags: every
// argument after it is positional, even one that starts with "-".
func parseInterspersed(fs *flag.FlagSet, args []string) ([]string, error) {
	var positional []string
	for {
		if err := fs.Parse(args); err != nil {
			return nil, err
		}
		rest := fs.Args()
		if len(rest) == 0 {
			return positional, nil
		}
		// The flag package consumes a "--" and stops there; it is gone from
		// rest, so it shows as one argument fewer than args held after the
		// flags it parsed.
		if consumed := len(args) - len(rest); consumed > 0 && args[consumed-1] == "--" {
			return append(positional, rest...), nil
		}
		positional = append(positional, rest[0])
		args = rest[1:]
	}
}

// backupLine is what backup prints for each tenant it stored.
type backupLine struct {
	ObjectKey       string  `json:"object_key"`
	Bytes           int64   `json:"bytes"`
	Nodes           int64   `json:"nodes"`
	Relationships   int64   `json:"relationships"`
	DurationSeconds float64 `json:"duration_seconds"`
}

// runTake stores a backup of each tenant, one after the other. A tenant
// that fails or is skipped does not stop the rest.
func runTake(ctx context.Context, logger *slog.Logger, stdout io.Writer, d deps, opts options) int {
	tenants, err := tenantsToBackUp(ctx, logger, d, opts)
	if err != nil {
		logger.Error("list the catalog's tenants", "error", err)
		return exitError
	}
	if len(tenants) == 0 {
		logger.Info("the catalog holds no tenant, so there is nothing to back up; " +
			"to back up a tenant that has not written yet, name it with --tenant <tenant>")
		return exitOK
	}

	var failed, skipped int
	enc := json.NewEncoder(stdout)
	notReached := d.taker.TakeEach(ctx, tenants, func(tenant string, res backup.Result, err error) {
		switch {
		case errors.Is(err, backup.ErrSkipped):
			logger.Warn("backup skipped: another backup or a restore holds the tenant's lease; "+
				"run it again once that ends (a lease a crashed holder left expires within minutes)", "tenant_id", tenant)
			skipped++
		case err != nil:
			logger.Error("backup failed: nothing was stored", "tenant_id", tenant, "error", err)
			failed++
		default:
			err = enc.Encode(backupLine{
				ObjectKey:       res.ObjectKey,
				Bytes:           res.Bytes,
				Nodes:           res.Nodes,
				Relationships:   res.Relationships,
				DurationSeconds: res.Duration.Seconds(),
			})
			if err != nil {
				logger.Error("write the result of the backup", "tenant_id", tenant, "object_key", res.ObjectKey, "error", err)
				failed++
			}
		}
	})
	if len(notReached) > 0 {
		logger.Error("interrupted: the remaining tenants were not backed up", "tenant_ids", notReached)
	}
	switch {
	case failed > 0 || len(notReached) > 0:
		return exitError
	case skipped > 0:
		return exitSkipped
	default:
		return exitOK
	}
}

// tenantsToBackUp returns the tenant --tenant names, or every tenant the
// catalog holds. A named tenant the catalog does not list holds no graph yet:
// it is backed up as an empty graph, which is how an install that has not
// written anything takes its first backup, and a warning tells a mistyped id
// from a new tenant.
func tenantsToBackUp(ctx context.Context, logger *slog.Logger, d deps, opts options) ([]string, error) {
	if !opts.tenantSet {
		return d.tenants.Tenants(ctx)
	}
	listed, err := d.tenants.Tenants(ctx)
	switch {
	case err != nil:
		logger.Warn("the catalog's tenants could not be listed; backing up the named tenant anyway", "tenant_id", opts.tenant, "error", err)
	case !slices.Contains(listed, opts.tenant):
		logger.Warn("tenant is not listed by the catalog, which holds no graph for it yet; backing it up as an empty graph",
			"tenant_id", opts.tenant, "known_tenants", strings.Join(listed, ","))
	}
	return []string{opts.tenant}, nil
}

// listLine is what list prints for each stored backup.
type listLine struct {
	Key          string `json:"key"`
	Size         int64  `json:"size"`
	LastModified string `json:"last_modified"`
}

// runList prints the stored backups of the named tenant, or of every tenant
// the catalog holds, newest first. A failure prints nothing, so a partial list
// is never taken for the whole one. A list with no --tenant that finds no
// tenant warns on stderr, since the bucket may still hold backups.
func runList(ctx context.Context, logger *slog.Logger, stdout io.Writer, d deps, opts options) int {
	tenants := []string{opts.tenant}
	if !opts.tenantSet {
		var err error
		if tenants, err = d.tenants.Tenants(ctx); err != nil {
			logger.Error("list the catalog's tenants", "error", err)
			return exitError
		}
		if len(tenants) == 0 {
			// The tenants come from the graph, which a Neo4j loss empties
			// while the bucket still holds the backups.
			logger.Warn("the catalog holds no tenant, so nothing was listed; " +
				"name one with --tenant <tenant> to list its backups")
		}
	}
	var all []ports.BackupObject
	for _, tenant := range tenants {
		objects, err := d.store.List(ctx, tenant)
		if err != nil {
			logger.Error("list the backups of the tenant", "tenant_id", tenant, "error", err)
			return exitError
		}
		all = append(all, objects...)
	}
	slices.SortStableFunc(all, func(a, b ports.BackupObject) int {
		return cmp.Or(b.TakenAt.Compare(a.TakenAt), strings.Compare(b.Key, a.Key))
	})
	out := make([]listLine, 0, len(all))
	for _, o := range all {
		out = append(out, listLine{
			Key:          o.Key,
			Size:         o.Size,
			LastModified: o.LastModified.UTC().Format(time.RFC3339),
		})
	}
	if err := json.NewEncoder(stdout).Encode(out); err != nil {
		logger.Error("write the list of backups", "error", err)
		return exitError
	}
	return exitOK
}

// restoreLine is what restore prints once it has loaded a backup.
type restoreLine struct {
	ObjectKey     string `json:"object_key"`
	TenantID      string `json:"tenant_id"`
	Nodes         int64  `json:"nodes"`
	Relationships int64  `json:"relationships"`
}

// runRestore returns the tenant the key names to the backup stored at it.
func runRestore(ctx context.Context, logger *slog.Logger, stdout io.Writer, d deps, opts options) int {
	res, err := d.restorer.Restore(ctx, opts.key)
	switch {
	case errors.Is(err, backup.ErrPartialRestore):
		logger.Error("restore failed after it wiped the graph: the graph of the tenant is partial; "+
			"run the same restore again, which wipes the tenant's graph first and repairs it",
			"tenant_id", opts.keyTenant, "object_key", opts.key, "error", err)
		return exitError
	case errors.Is(err, backup.ErrSkipped):
		logger.Warn("restore skipped: another backup or a restore holds the tenant's lease; "+
			"run it again once that ends (a lease a crashed holder left expires within minutes)",
			"tenant_id", opts.keyTenant, "object_key", opts.key)
		return exitSkipped
	case errors.Is(err, ports.ErrBackupNotFound):
		logger.Error("restore failed: no backup is stored at the key, and the graph is untouched; "+
			"`catalog-backup list --tenant "+opts.keyTenant+"` lists the tenant's backups",
			"tenant_id", opts.keyTenant, "object_key", opts.key, "error", err)
		return exitError
	case errors.Is(err, ports.ErrNotABackupKey):
		logger.Error("usage", "object_key", opts.key, "error", err)
		return exitUsage
	case err != nil:
		logger.Error("restore failed", "tenant_id", opts.keyTenant, "object_key", opts.key, "error", err)
		return exitError
	}
	err = json.NewEncoder(stdout).Encode(restoreLine{
		ObjectKey:     opts.key,
		TenantID:      opts.keyTenant,
		Nodes:         res.Nodes,
		Relationships: res.Relationships,
	})
	if err != nil {
		logger.Error("write the result of the restore", "object_key", opts.key, "error", err)
		return exitError
	}
	return exitOK
}

// settings are the values connect needs: catalog's Neo4j and S3 settings and
// the keep a backup prunes with.
type settings struct {
	neo4j config.Neo4jConfig
	s3    pkgconfig.S3Config
	keep  int
}

// loadConfig reads the same Neo4j and S3 settings catalog does, through the
// same loaders, and CATALOG_BACKUP_KEEP (default 7). It names every key that
// is missing or holds a value it cannot use, and reads nothing else of
// catalog's configuration.
func loadConfig() (settings, error) {
	v := &pkgconfig.Validator{}
	s := settings{
		neo4j: config.LoadNeo4j(v),
		s3:    pkgconfig.LoadS3(v),
		keep:  config.LoadBackupKeep(v),
	}
	if missing := v.Missing(); len(missing) > 0 {
		return settings{}, fmt.Errorf("missing or invalid env vars: %s", strings.Join(missing, ", "))
	}
	return s, nil
}

// neo4jConnectPolicy is how long the command waits for a Neo4j that is still
// starting before it gives up.
func neo4jConnectPolicy() neo4jinfra.StartupPolicy {
	return neo4jinfra.StartupPolicy{Timeout: 30 * time.Second, InitialDelay: time.Second, MaxDelay: 5 * time.Second}
}

// connect builds the taker, the restorer, the backup store and the tenant
// list over catalog's Neo4j and object store.
func connect(ctx context.Context, logger *slog.Logger) (deps, func(), error) {
	cfg, err := loadConfig()
	if err != nil {
		return deps{}, nil, err
	}
	client, err := neo4jinfra.NewNeo4jClientWithRetry(ctx, cfg.neo4j.URI, cfg.neo4j.User, cfg.neo4j.Password, logger, neo4jConnectPolicy())
	if err != nil {
		return deps{}, nil, fmt.Errorf("connect to Neo4j: %w", err)
	}
	objects, err := objectstore.New(ctx, objectstore.ConfigFromS3(cfg.s3))
	if err != nil {
		_ = client.Close(context.WithoutCancel(ctx))
		return deps{}, nil, fmt.Errorf("connect to object storage: %w", err)
	}

	store := s3infra.NewBackupStore(objects)
	lease := neo4jinfra.NewBackupLease(client, logger)
	d := deps{
		taker:    backup.NewTaker(lease, neo4jinfra.NewGraphSaver(client, config.Version(), logger), store, metricsinfra.NopBackupMetrics{}, cfg.keep, time.Now, logger),
		restorer: backup.NewRestorer(lease, neo4jinfra.NewGraphLoader(client, logger), store, logger),
		store:    store,
		tenants:  neo4jinfra.NewGraphTenants(client),
	}
	release := func() {
		// The work may have ended because the context was cancelled; the
		// connection still closes cleanly.
		closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		if err := client.Close(closeCtx); err != nil {
			logger.Warn("close the Neo4j connection", "error", err)
		}
	}
	return d, release, nil
}
