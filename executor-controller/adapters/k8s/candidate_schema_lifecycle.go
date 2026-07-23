package k8s

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"regexp"
	"strings"

	"github.com/carolsimone/continuo/executor-controller/service/ports"
)

// candidateSchemaNameRe is the strict allowlist for candidate schema names: the literal
// "_candidate_" prefix followed by one or more [a-zA-Z0-9_] characters. The name reaches
// the engine image as an env value, so validating it here keeps a malformed name from
// ever being scheduled.
var candidateSchemaNameRe = regexp.MustCompile(`^_candidate_[a-zA-Z0-9_]+$`)

// dnsUnsafe matches runs of characters not allowed in a DNS-1123 subdomain (a k8s
// object name); candidate schema names carry underscores, which names forbid.
var dnsUnsafe = regexp.MustCompile(`[^a-z0-9-]+`)

// candidateSchemaJobRunner implements the candidate-schema lifecycle ports by scheduling
// one-shot engine-image Jobs (harness ensure_schema/drop_schema ops) and blocking on the
// result. The executor never connects to the warehouse; the engine adapter baked into
// VALIDATION_IMAGE owns the DDL dialect.
type candidateSchemaJobRunner struct {
	client    *K8sClient
	namespace string
	logger    *slog.Logger
}

var (
	_ ports.CandidateSchemaCreator = (*candidateSchemaJobRunner)(nil)
	_ ports.CandidateSchemaCleaner = (*candidateSchemaJobRunner)(nil)
)

// NewCandidateSchemaCreator returns a creator that schedules an ensure_schema
// engine-image Job and blocks until it succeeds.
func NewCandidateSchemaCreator(client *K8sClient, namespace string, logger *slog.Logger) ports.CandidateSchemaCreator {
	return &candidateSchemaJobRunner{client: client, namespace: namespace, logger: logger}
}

// NewCandidateSchemaCleaner returns a cleaner that schedules a drop_schema engine-image Job.
func NewCandidateSchemaCleaner(client *K8sClient, namespace string, logger *slog.Logger) ports.CandidateSchemaCleaner {
	return &candidateSchemaJobRunner{client: client, namespace: namespace, logger: logger}
}

// EnsureCandidateSchema schedules an ensure_schema Job and blocks until it succeeds.
func (r *candidateSchemaJobRunner) EnsureCandidateSchema(ctx context.Context, schema string) error {
	return r.run(ctx, schemaOpEnsure, schema)
}

// DropCandidateSchema schedules a drop_schema Job and blocks until it succeeds.
func (r *candidateSchemaJobRunner) DropCandidateSchema(ctx context.Context, schema string) error {
	return r.run(ctx, schemaOpDrop, schema)
}

func (r *candidateSchemaJobRunner) run(ctx context.Context, op, schema string) error {
	if !candidateSchemaNameRe.MatchString(schema) {
		return fmt.Errorf("refusing to run %s on non-candidate schema %q", op, schema)
	}
	return r.client.RunSchemaOpJob(ctx, op, schema, schemaOpJobName(op, schema), r.namespace)
}

// maxSchemaOpJobNameLen caps the schema-op Job name. Kubernetes copies the Job name
// into the pod-template `job-name` label (63-char limit) and derives pod names /
// hostnames from it, so the cap stays under 63 with headroom for the pod-name suffix.
// A 40-char commit-SHA release id already produces a 64-char untruncated name.
const maxSchemaOpJobNameLen = 52

// schemaOpJobName derives a deterministic, DNS-1123-safe Job name from the op and
// candidate schema, so a redelivered trigger maps to the same (idempotent) Job. The
// literal prefix guarantees the name never starts with a sanitized dash. Names over
// maxSchemaOpJobNameLen keep a readable head and regain uniqueness with a digest of
// the full schema name — distinct schemas sharing a truncated head must never map to
// one Job.
func schemaOpJobName(op, schema string) string {
	prefix := "ensure"
	if op == schemaOpDrop {
		prefix = "drop"
	}
	name := fmt.Sprintf("%s-schema-%s", prefix, sanitizeK8sName(schema))
	if len(name) <= maxSchemaOpJobNameLen {
		return name
	}
	digest := sha256.Sum256([]byte(schema))
	suffix := hex.EncodeToString(digest[:])[:10]
	head := strings.TrimRight(name[:maxSchemaOpJobNameLen-len(suffix)-1], "-")
	return head + "-" + suffix
}

// sanitizeK8sName lowercases s and reduces it to a DNS-1123-safe fragment. It is used
// only inside a prefixed Job name, so trimmed dashes here cannot leave the whole name
// starting or ending with a dash.
func sanitizeK8sName(s string) string {
	return strings.Trim(dnsUnsafe.ReplaceAllString(strings.ToLower(s), "-"), "-")
}
