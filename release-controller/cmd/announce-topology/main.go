// announce-topology announces a topology to the rest of continuo on
// release.promoted:v2 under a fresh promotion seq, without a release and
// without moving current_prod. The benchmark and e2e harnesses use it to load
// a topology, and to restore the live one afterwards.
//
//	announce-topology --release-id bench-20261008 --topology nodes.json
//	announce-topology --reannounce-current
//	announce-topology --print-current > live.json
//
// --topology reads a JSON array of topology artifact nodes from a path, or
// from stdin with "-". --reannounce-current announces current_prod's own
// topology again. --print-current writes current_prod's topology nodes to
// stdout and announces nothing.
//
// stdout carries only the result: one JSON object
// {"release_id","promotion_seq","topology_uri","topology_sha256"}, or the
// node array for --print-current. Diagnostics go to stderr. Exit codes: 0 ok,
// 1 error, 2 usage (bad flags, an unreadable topology, a release id that
// already names a run or current_prod), 3 nothing has been promoted.
//
// The command writes the artifact to S3 and the announcement to
// release-controller's outbox; the running release-controller publishes it.
// It reads the same POSTGRES_* and S3 environment as release-controller.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"

	pkgconfig "github.com/carolsimone/continuo/pkg/config"
	pkgdb "github.com/carolsimone/continuo/pkg/db"
	"github.com/carolsimone/continuo/pkg/topologyartifact"
	"github.com/carolsimone/continuo/release-controller/adapters/postgres"
	s3adapter "github.com/carolsimone/continuo/release-controller/adapters/s3"
	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/service/handlers"
	"github.com/carolsimone/continuo/release-controller/service/ports"
	"github.com/carolsimone/continuo/release-controller/service/uow"
)

const (
	exitOK            = 0
	exitError         = 1
	exitUsage         = 2
	exitNoCurrentProd = 3
)

// topologyAnnouncer is the part of release-controller the command drives.
type topologyAnnouncer interface {
	Announce(ctx context.Context, releaseID string, topo release.Topology) (handlers.AnnounceResult, error)
	ReannounceCurrent(ctx context.Context) (handlers.AnnounceResult, error)
	CurrentTopology(ctx context.Context) (release.Topology, error)
}

// connectFunc opens the announcer and returns a function that releases it.
type connectFunc func(ctx context.Context) (topologyAnnouncer, func(), error)

type options struct {
	releaseID         string
	topologyPath      string
	reannounceCurrent bool
	printCurrent      bool
}

func main() {
	os.Exit(run(context.Background(), os.Args[1:], os.Stdin, os.Stdout, os.Stderr, connect))
}

func run(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer, connect connectFunc) int {
	logger := slog.New(slog.NewTextHandler(stderr, nil))

	opts, err := parseFlags(args, stderr)
	if errors.Is(err, flag.ErrHelp) {
		return exitOK
	}
	if err != nil {
		logger.Error("usage", "error", err)
		return exitUsage
	}

	var topo release.Topology
	if opts.topologyPath != "" {
		in := stdin
		if opts.topologyPath != "-" {
			f, err := os.Open(opts.topologyPath)
			if err != nil {
				logger.Error("open topology", "path", opts.topologyPath, "error", err)
				return exitUsage
			}
			defer func() { _ = f.Close() }()
			in = f
		}
		if topo, err = readTopology(in); err != nil {
			logger.Error("read topology", "error", err)
			return exitUsage
		}
	}

	a, closeFn, err := connect(ctx)
	if err != nil {
		logger.Error("connect", "error", err)
		return exitError
	}
	defer closeFn()

	if opts.printCurrent {
		current, err := a.CurrentTopology(ctx)
		if err != nil {
			return failure(logger, "print current_prod topology", err)
		}
		return writeJSON(stdout, logger, fromTopology(current))
	}

	var res handlers.AnnounceResult
	if opts.reannounceCurrent {
		res, err = a.ReannounceCurrent(ctx)
	} else {
		res, err = a.Announce(ctx, opts.releaseID, topo)
	}
	if err != nil {
		return failure(logger, "announce topology", err)
	}
	return writeJSON(stdout, logger, struct {
		ReleaseID      string `json:"release_id"`
		PromotionSeq   int64  `json:"promotion_seq"`
		TopologyURI    string `json:"topology_uri"`
		TopologySHA256 string `json:"topology_sha256"`
	}{res.ReleaseID, res.PromotionSeq, res.TopologyURI, res.TopologySHA256})
}

func parseFlags(args []string, stderr io.Writer) (options, error) {
	fs := flag.NewFlagSet("announce-topology", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var o options
	fs.StringVar(&o.releaseID, "release-id", "", "release id to announce the topology under (with --topology)")
	fs.StringVar(&o.topologyPath, "topology", "", `topology to announce: a JSON array of topology artifact nodes, read from this path or from stdin with "-"`)
	fs.BoolVar(&o.reannounceCurrent, "reannounce-current", false, "announce current_prod's own topology again under a fresh promotion seq")
	fs.BoolVar(&o.printCurrent, "print-current", false, "write current_prod's topology nodes to stdout as a JSON array and announce nothing")
	if err := fs.Parse(args); err != nil {
		return options{}, err
	}
	if fs.NArg() > 0 {
		return options{}, fmt.Errorf("unexpected arguments: %s", strings.Join(fs.Args(), " "))
	}
	modes := 0
	if o.topologyPath != "" || o.releaseID != "" {
		modes++
	}
	if o.reannounceCurrent {
		modes++
	}
	if o.printCurrent {
		modes++
	}
	if modes != 1 {
		return options{}, errors.New("choose exactly one of --topology with --release-id, --reannounce-current, --print-current")
	}
	if (o.topologyPath == "") != (o.releaseID == "") {
		return options{}, errors.New("--topology and --release-id go together")
	}
	return o, nil
}

// readTopology decodes a JSON array of topology artifact nodes. Unknown fields
// are refused, so a misspelt or retired field fails here instead of being
// dropped silently.
func readTopology(r io.Reader) (release.Topology, error) {
	dec := json.NewDecoder(r)
	dec.DisallowUnknownFields()
	var nodes []topologyartifact.Node
	if err := dec.Decode(&nodes); err != nil {
		return nil, fmt.Errorf("decode topology nodes: %w", err)
	}
	if len(nodes) == 0 {
		return nil, errors.New("the topology has no nodes; announcing it would retire every live node")
	}
	seen := make(map[string]bool, len(nodes))
	topo := make(release.Topology, 0, len(nodes))
	for i, n := range nodes {
		if n.UniqueID == "" {
			return nil, fmt.Errorf("node %d has no unique_id", i)
		}
		if seen[n.UniqueID] {
			return nil, fmt.Errorf("unique_id %q appears more than once", n.UniqueID)
		}
		seen[n.UniqueID] = true
		upstreams := n.UpstreamUniqueIDs
		if upstreams == nil {
			upstreams = []string{}
		}
		topo = append(topo, release.Node{
			UniqueID:           n.UniqueID,
			SchemaName:         n.SchemaName,
			TableName:          n.TableName,
			ResolvedRelationID: n.ResolvedRelationID,
			ServiceName:        n.ServiceName,
			NodeType:           n.NodeType,
			ContentHash:        n.ContentHash,
			TestCount:          n.TestCount,
			ImageTag:           n.ImageTag,
			UpstreamUniqueIDs:  upstreams,
			Schedule:           n.Schedule,
			OriginalFilePath:   n.OriginalFilePath,
			SecretRef:          n.SecretRef,
		})
	}
	return topo, nil
}

// fromTopology renders a topology as the artifact nodes --topology reads.
func fromTopology(topo release.Topology) []topologyartifact.Node {
	nodes := make([]topologyartifact.Node, 0, len(topo))
	for _, n := range topo {
		nodes = append(nodes, topologyartifact.Node{
			UniqueID:           n.UniqueID,
			SchemaName:         n.SchemaName,
			TableName:          n.TableName,
			ResolvedRelationID: n.ResolvedRelationID,
			ServiceName:        n.ServiceName,
			NodeType:           n.NodeType,
			TestCount:          n.TestCount,
			ContentHash:        n.ContentHash,
			ImageTag:           n.ImageTag,
			OriginalFilePath:   n.OriginalFilePath,
			UpstreamUniqueIDs:  n.UpstreamUniqueIDs,
			Schedule:           n.Schedule,
			SecretRef:          n.SecretRef,
		})
	}
	return nodes
}

func failure(logger *slog.Logger, what string, err error) int {
	logger.Error(what, "error", err)
	switch {
	case errors.Is(err, handlers.ErrNoCurrentProd):
		return exitNoCurrentProd
	case errors.Is(err, handlers.ErrReleaseIDTaken), errors.Is(err, handlers.ErrInvalidReleaseID):
		return exitUsage
	default:
		return exitError
	}
}

func writeJSON(w io.Writer, logger *slog.Logger, v any) int {
	if err := json.NewEncoder(w).Encode(v); err != nil {
		logger.Error("write result", "error", err)
		return exitError
	}
	return exitOK
}

// depsAnnouncer drives release-controller's use cases over its own Postgres
// and S3.
type depsAnnouncer struct{ d *handlers.Deps }

func (a depsAnnouncer) Announce(ctx context.Context, releaseID string, topo release.Topology) (handlers.AnnounceResult, error) {
	return handlers.AnnounceTopology(ctx, a.d, releaseID, topo)
}

func (a depsAnnouncer) ReannounceCurrent(ctx context.Context) (handlers.AnnounceResult, error) {
	return handlers.ReannounceCurrentProd(ctx, a.d)
}

func (a depsAnnouncer) CurrentTopology(ctx context.Context) (release.Topology, error) {
	return handlers.CurrentProdTopology(ctx, a.d)
}

func connect(ctx context.Context) (topologyAnnouncer, func(), error) {
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	pgCfg, s3Cfg, err := loadConfig()
	if err != nil {
		return nil, nil, err
	}
	db, err := pkgdb.Open(ctx, pgCfg, pkgconfig.PoolConfig{MaxOpenConns: 2, MaxIdleConns: 1})
	if err != nil {
		return nil, nil, fmt.Errorf("connect to PostgreSQL: %w", err)
	}
	s3Client, err := s3adapter.NewS3Client(ctx, s3Cfg.EndpointURL, s3Cfg.Bucket, s3Cfg.Region, s3Cfg.AccessKeyID, s3Cfg.SecretAccessKey, logger)
	if err != nil {
		_ = db.Close()
		return nil, nil, fmt.Errorf("connect to object storage: %w", err)
	}
	deps := &handlers.Deps{
		NewUoW:     func() uow.UnitOfWork { return postgres.NewUnitOfWork(db, logger, s3Client) },
		Clock:      ports.SystemClock{},
		Telemetry:  ports.NoOpTelemetry{},
		Logger:     logger,
		Bucket:     s3Cfg.Bucket,
		Topologies: s3adapter.NewTopologyArtifactStore(s3Client, s3Cfg.Bucket, 1),
	}
	return depsAnnouncer{d: deps}, func() { _ = db.Close() }, nil
}

// loadConfig reads release-controller's connection settings: POSTGRES_HOST,
// POSTGRES_USER, POSTGRES_PASSWORD, S3_ENDPOINT_URL, S3_BUCKET and
// AWS_DEFAULT_REGION are required; POSTGRES_PORT defaults to 5432,
// POSTGRES_DB to continuo_release and DB_SSLMODE to disable. It names every
// key that is missing or holds a value it cannot use.
func loadConfig() (pkgconfig.PostgresConfig, pkgconfig.S3Config, error) {
	v := &pkgconfig.Validator{}
	pg := pkgconfig.PostgresConfig{
		Host:     v.Require("POSTGRES_HOST"),
		Port:     v.PortOrDefault("POSTGRES_PORT", 5432),
		User:     v.Require("POSTGRES_USER"),
		Password: v.Require("POSTGRES_PASSWORD"),
		DB:       pkgconfig.EnvOrDefault("POSTGRES_DB", "continuo_release"),
		SSLMode:  pkgconfig.EnvOrDefault("DB_SSLMODE", "disable"),
	}
	s3 := pkgconfig.LoadS3(v)
	if missing := v.Missing(); len(missing) > 0 {
		return pkgconfig.PostgresConfig{}, pkgconfig.S3Config{}, fmt.Errorf("missing or invalid env vars: %s", strings.Join(missing, ", "))
	}
	return pg, s3, nil
}
