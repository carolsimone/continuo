// Package contractbundle is the shared reader for the contract bundle: the
// one immutable, gzipped JSON document that carries the data contracts of a
// release, stored at tenants/<tenant>/topologies/<release>/contracts.json.gz
// next to the release's topology artifact.
//
// topology-controller writes it, as canonical JSON (RFC 8785, the
// continuo-contracts library's canonical_json) gzipped with a zero modification
// time and stored as it is, with Content-Type application/gzip and no
// Content-Encoding, so no client or proxy decompresses it on the way. Events
// carry the object's URI and the SHA-256 of those stored bytes, and every
// reader verifies the checksum before it trusts a byte. testdata/contracts_v1.json
// holds the document one release must produce; topology-controller's tests read
// a byte-identical copy of it.
package contractbundle

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/carolsimone/continuo/pkg/domain/model"
)

// SchemaVersion is the only document schema_version this build reads. A
// document of another version is rejected, never half-read.
const SchemaVersion = 1

// MaxObjectBytes caps the gzipped object a reader accepts.
const MaxObjectBytes = 64 << 20

// MaxDocumentBytes caps the gunzipped JSON a reader accepts.
const MaxDocumentBytes = 512 << 20

// Document is the whole bundle of one release. Ontology, Failures and Timings
// are kept as the JSON the writer produced: a reader of the contracts does not
// interpret them, except that OntologyVersion names the ontology the contracts
// were resolved against.
type Document struct {
	SchemaVersion     int                          `json:"schema_version"`
	TenantID          string                       `json:"tenant_id"`
	ReleaseID         string                       `json:"release_id"`
	ODCSSchemaSHA256  string                       `json:"odcs_schema_sha256"`
	ContinuoContracts string                       `json:"continuo_contracts"`
	Ontology          json.RawMessage              `json:"ontology"`
	Provenance        map[string]ServiceProvenance `json:"provenance"`
	Summary           Summary                      `json:"summary"`
	Failures          json.RawMessage              `json:"failures"`
	Timings           json.RawMessage              `json:"timings"`
	// Contracts holds one entry per node, keyed by the node's unique id
	// ("<schema>.<table>").
	Contracts map[string]Contract `json:"contracts"`
}

// OntologyVersion names the ontology the bundle's contracts were resolved
// against: "<release_id>@sha256:<hex>", the release of the ontology service that
// wrote the file and the SHA-256 of the file's text. It is empty when the
// bundle names no ontology release: the install's ontology switch was off, or it
// was on and no ontology was released. A reader never has to tell the two
// apart, and a value it cannot read as a release id and a digest names none.
func (d Document) OntologyVersion() string {
	var ontology struct {
		ReleaseID string `json:"release_id"`
		SHA256    string `json:"sha256"`
	}
	if len(d.Ontology) == 0 || json.Unmarshal(d.Ontology, &ontology) != nil {
		return ""
	}
	if ontology.ReleaseID == "" || ontology.SHA256 == "" {
		return ""
	}
	return ontology.ReleaseID + "@sha256:" + ontology.SHA256
}

// ServiceProvenance records what built a service's Python contracts: the
// runtime that packed them and the packages of the service's image.
type ServiceProvenance struct {
	ContinuoPythonRuntime string            `json:"continuo_python_runtime"`
	ImagePackages         map[string]string `json:"image_packages"`
}

// Summary counts the bundle's contracts, in all and by family, the contracts
// that failed validation (Invalid) and the nodes the release left out (Skipped).
type Summary struct {
	Contracts int                          `json:"contracts"`
	ByFamily  map[model.ContractFamily]int `json:"by_family"`
	Invalid   int                          `json:"invalid"`
	Skipped   int                          `json:"skipped"`
}

// Contract is one node's contract. Family names the producer that wrote it,
// `dbt` for a dbt model, seed or snapshot and `python` for a Python node (see
// model.ContractFamily), and the reader keeps it as the writer wrote it. Document is its stored form, the
// ODCS document the hashes are taken from, kept as the JSON the writer
// produced. ContractHash is the hash of that document and ContentHash the
// node's code fingerprint.
type Contract struct {
	Family       model.ContractFamily `json:"family"`
	Service      string               `json:"service"`
	ContractHash string               `json:"contract_hash"`
	ContentHash  string               `json:"content_hash"`
	Source       Source               `json:"source"`
	Document     json.RawMessage      `json:"document"`
}

// Source is the file a contract stands for: its path in the service's
// repository and the SHA-256 of what it was written from (a Python contract
// file's text, or the manifest fields the dbt generator read).
type Source struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
}

// ErrChecksumMismatch reports an object whose SHA-256 differs from the one its
// event announced.
var ErrChecksumMismatch = errors.New("contract bundle checksum mismatch")

// ErrTooLarge reports an object over MaxObjectBytes or a document over
// MaxDocumentBytes.
var ErrTooLarge = errors.New("contract bundle too large")

// ErrMalformed reports an object no amount of re-reading can make readable: not
// gzip, not a JSON object of the expected shape, no release id, or a contract
// without a unique id.
var ErrMalformed = errors.New("malformed contract bundle")

// ErrUnsupportedVersion reports a document whose schema_version is not
// SchemaVersion.
var ErrUnsupportedVersion = errors.New("unsupported contract bundle schema version")

// Key is the S3 object key of a release's contract bundle.
func Key(tenantID, releaseID string) string {
	return fmt.Sprintf("tenants/%s/topologies/%s/contracts.json.gz", tenantID, releaseID)
}

// Decode verifies gz against wantSHA256, then gunzips and parses it. The
// checksum is checked before anything else, so a corrupt object is never
// unzipped. Every error wraps ErrChecksumMismatch, ErrTooLarge, ErrMalformed or
// ErrUnsupportedVersion; none goes away on a re-read, so a consumer treats all
// four as permanent. Keys the document holds beyond those of Document are
// ignored.
func Decode(gz []byte, wantSHA256 string) (Document, error) {
	return decode(gz, wantSHA256, MaxObjectBytes, MaxDocumentBytes)
}

// decode is Decode with the two size limits given, so a test can bound the
// gunzipped document without building one of MaxDocumentBytes.
func decode(gz []byte, wantSHA256 string, maxObjectBytes, maxDocumentBytes int) (Document, error) {
	sum := sha256.Sum256(gz)
	if got := hex.EncodeToString(sum[:]); got != strings.ToLower(wantSHA256) {
		return Document{}, fmt.Errorf("%w: want %s, got %s", ErrChecksumMismatch, wantSHA256, got)
	}
	if len(gz) > maxObjectBytes {
		return Document{}, fmt.Errorf("%w: object is %d bytes, the limit is %d", ErrTooLarge, len(gz), maxObjectBytes)
	}
	zr, err := gzip.NewReader(bytes.NewReader(gz))
	if err != nil {
		return Document{}, fmt.Errorf("%w: gzip: %v", ErrMalformed, err)
	}
	raw, err := io.ReadAll(io.LimitReader(zr, int64(maxDocumentBytes)+1))
	if err != nil {
		return Document{}, fmt.Errorf("%w: gunzip: %v", ErrMalformed, err)
	}
	if len(raw) > maxDocumentBytes {
		return Document{}, fmt.Errorf("%w: document exceeds %d bytes", ErrTooLarge, maxDocumentBytes)
	}
	// A document of another version may be shaped differently, so its version
	// is read before the rest of it.
	var header struct {
		SchemaVersion int `json:"schema_version"`
	}
	if err := json.Unmarshal(raw, &header); err != nil {
		return Document{}, fmt.Errorf("%w: json: %v", ErrMalformed, err)
	}
	if header.SchemaVersion != SchemaVersion {
		return Document{}, fmt.Errorf("%w: schema_version %d (this build reads %d)", ErrUnsupportedVersion, header.SchemaVersion, SchemaVersion)
	}
	var d Document
	if err := json.Unmarshal(raw, &d); err != nil {
		return Document{}, fmt.Errorf("%w: json: %v", ErrMalformed, err)
	}
	if d.ReleaseID == "" {
		return Document{}, fmt.Errorf("%w: empty release_id", ErrMalformed)
	}
	if _, ok := d.Contracts[""]; ok {
		return Document{}, fmt.Errorf("%w: a contract has no unique id", ErrMalformed)
	}
	return d, nil
}
