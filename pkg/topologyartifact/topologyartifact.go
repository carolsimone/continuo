// Package topologyartifact is the shared codec for the topology artifact: the
// one immutable, gzipped JSON document that carries a release's whole topology,
// stored at tenants/<tenant>/topologies/<release>/topology.json.gz.
//
// topology-controller writes it for every candidate and verification run;
// release-controller writes it for the topologies it announces itself. A
// writer stores Encode's bytes as they are, with Content-Type application/gzip
// and no Content-Encoding, so no client or proxy decompresses them on the way.
// Events carry the object's URI and the SHA-256 of those stored bytes, never
// the topology, and every reader verifies the checksum before it trusts a byte.
package topologyartifact

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"

	"github.com/carolsimone/continuo/pkg/domain/model"
)

// SchemaVersion is the only document schema_version this build writes and
// reads. A document of another version is rejected, never half-read.
const SchemaVersion = 1

// MaxObjectBytes caps the gzipped object a reader accepts.
const MaxObjectBytes = 64 << 20

// MaxDocumentBytes caps the gunzipped JSON a reader accepts.
const MaxDocumentBytes = 512 << 20

// Node is one node of a release's topology. UniqueID is "<schema>.<table>",
// lowercased, except for a dbt test, which keeps its dbt unique id.
type Node struct {
	UniqueID           string   `json:"unique_id"`
	SchemaName         string   `json:"schema_name"`
	TableName          string   `json:"table_name"`
	ResolvedRelationID string   `json:"resolved_relation_id"`
	ServiceName        string   `json:"service_name"`
	NodeType           string   `json:"node_type"`
	TestCount          int      `json:"test_count"`
	ContentHash        string   `json:"content_hash"`
	ImageTag           string   `json:"image_tag"`
	OriginalFilePath   string   `json:"original_file_path"`
	UpstreamUniqueIDs  []string `json:"upstream_unique_ids"`
	Schedule           string   `json:"schedule"`
	SecretRef          string   `json:"secret_ref,omitempty"`
}

// Document is the whole artifact of one release.
type Document struct {
	SchemaVersion int    `json:"schema_version"`
	TenantID      string `json:"tenant_id"`
	ReleaseID     string `json:"release_id"`
	Nodes         []Node `json:"nodes"`
}

// ErrChecksumMismatch reports an object whose SHA-256 differs from the one its
// event announced.
var ErrChecksumMismatch = errors.New("topology artifact checksum mismatch")

// ErrMalformed reports an object no amount of re-reading can make readable:
// oversized, not gzip, not JSON, an unsupported schema_version, no release id,
// or a node without a unique id. Two nodes that share a unique id are not
// malformed: the codec keeps both and the release's collision check decides.
var ErrMalformed = errors.New("malformed topology artifact")

// Key is the S3 object key of a release's artifact.
func Key(tenantID, releaseID string) string {
	return fmt.Sprintf("tenants/%s/topologies/%s/topology.json.gz", tenantID, releaseID)
}

// CanonicalJSON renders d in the one byte form every writer produces: nodes
// ordered by unique_id (a stable sort, so equal ids keep their input order),
// each node's upstream ids sorted, object keys sorted, compact separators,
// UTF-8 text without HTML escaping, secret_ref omitted when empty,
// schema_version set to SchemaVersion, no trailing newline.
// encoding/json writes the line and paragraph separators U+2028 and U+2029 as
// six-character escape sequences, so a writer in another language must too.
func CanonicalJSON(d Document) ([]byte, error) {
	nodes := make([]map[string]any, 0, len(d.Nodes))
	for _, n := range d.Nodes {
		upstreams := slices.Clone(n.UpstreamUniqueIDs)
		if upstreams == nil {
			upstreams = []string{}
		}
		slices.Sort(upstreams)
		node := map[string]any{
			"unique_id":            n.UniqueID,
			"schema_name":          n.SchemaName,
			"table_name":           n.TableName,
			"resolved_relation_id": n.ResolvedRelationID,
			"service_name":         n.ServiceName,
			"node_type":            n.NodeType,
			"test_count":           n.TestCount,
			"content_hash":         n.ContentHash,
			"image_tag":            n.ImageTag,
			"original_file_path":   n.OriginalFilePath,
			"upstream_unique_ids":  upstreams,
			"schedule":             n.Schedule,
		}
		if n.SecretRef != "" {
			node["secret_ref"] = n.SecretRef
		}
		nodes = append(nodes, node)
	}
	slices.SortStableFunc(nodes, func(a, b map[string]any) int {
		return strings.Compare(a["unique_id"].(string), b["unique_id"].(string))
	})
	doc := map[string]any{
		"schema_version": SchemaVersion,
		"tenant_id":      d.TenantID,
		"release_id":     d.ReleaseID,
		"nodes":          nodes,
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(doc); err != nil {
		return nil, fmt.Errorf("encode topology artifact: %w", err)
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n")), nil
}

// Encode returns the gzipped canonical JSON of d and the lowercase hex SHA-256
// of those gzipped bytes. The gzip header carries no modification time and no
// file name, so one document always encodes to the same bytes.
func Encode(d Document) ([]byte, string, error) {
	raw, err := CanonicalJSON(d)
	if err != nil {
		return nil, "", err
	}
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write(raw); err != nil {
		return nil, "", fmt.Errorf("gzip topology artifact: %w", err)
	}
	if err := zw.Close(); err != nil {
		return nil, "", fmt.Errorf("gzip topology artifact: %w", err)
	}
	sum := sha256.Sum256(buf.Bytes())
	return buf.Bytes(), hex.EncodeToString(sum[:]), nil
}

// Decode verifies gz against wantSHA256, then gunzips and parses it. Every
// error wraps ErrChecksumMismatch or ErrMalformed; neither goes away on a
// re-read, so a consumer treats both as permanent.
func Decode(gz []byte, wantSHA256 string) (Document, error) {
	if len(gz) > MaxObjectBytes {
		return Document{}, fmt.Errorf("%w: object is %d bytes, the limit is %d", ErrMalformed, len(gz), MaxObjectBytes)
	}
	sum := sha256.Sum256(gz)
	if got := hex.EncodeToString(sum[:]); got != strings.ToLower(wantSHA256) {
		return Document{}, fmt.Errorf("%w: want %s, got %s", ErrChecksumMismatch, wantSHA256, got)
	}
	zr, err := gzip.NewReader(bytes.NewReader(gz))
	if err != nil {
		return Document{}, fmt.Errorf("%w: gzip: %v", ErrMalformed, err)
	}
	raw, err := io.ReadAll(io.LimitReader(zr, MaxDocumentBytes+1))
	if err != nil {
		return Document{}, fmt.Errorf("%w: gunzip: %v", ErrMalformed, err)
	}
	if len(raw) > MaxDocumentBytes {
		return Document{}, fmt.Errorf("%w: document exceeds %d bytes", ErrMalformed, MaxDocumentBytes)
	}
	var d Document
	if err := json.Unmarshal(raw, &d); err != nil {
		return Document{}, fmt.Errorf("%w: json: %v", ErrMalformed, err)
	}
	if d.SchemaVersion != SchemaVersion {
		return Document{}, fmt.Errorf("%w: schema_version %d (this build reads %d)", ErrMalformed, d.SchemaVersion, SchemaVersion)
	}
	if d.ReleaseID == "" {
		return Document{}, fmt.Errorf("%w: empty release_id", ErrMalformed)
	}
	for i, n := range d.Nodes {
		if n.UniqueID == "" {
			return Document{}, fmt.Errorf("%w: node %d has no unique_id", ErrMalformed, i)
		}
	}
	return d, nil
}

// CandidateObjectKey is the S3 key of the object a node's validation Job
// fetches: the compiled SQL rewritten to the candidate schema for a dbt node
// (.sql), the validation spec for a python node (.json). topology-controller
// writes these objects; a dbt seed has none, and neither does a node type this
// build does not know, so both return "".
func CandidateObjectKey(releaseID, uniqueID, nodeType string) string {
	t := model.NodeType(nodeType)
	if !t.IsValid() || t == model.NodeTypeDbtSeed {
		return ""
	}
	ext := "sql"
	if t.Runtime() == model.NodeRuntimePython {
		ext = "json"
	}
	return fmt.Sprintf("candidate-sql/%s/candidate_%s.%s", releaseID, uniqueID, ext)
}
