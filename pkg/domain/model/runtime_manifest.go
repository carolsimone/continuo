package model

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
)

// RuntimeManifestFormatV1 is the format identifier of a runtime manifest
// artifact: a dbt partial-parse msgpack file produced by compiling a service's
// dbt project. It is stamped on every descriptor so a consumer can reject an
// artifact whose layout it does not understand.
const RuntimeManifestFormatV1 = "dbt-partial-parse-msgpack-v1"

// RuntimeManifestRef points at the runtime manifest a node should execute
// against. It is embedded in the events services exchange, so all four fields
// are set together or none are: a partial reference is a contract violation,
// not a degraded mode.
type RuntimeManifestRef struct {
	// RuntimeManifestURI is the s3:// location of the artifact.
	RuntimeManifestURI string `json:"runtime_manifest_uri,omitempty"`
	// RuntimeManifestSHA256 is the artifact's content digest, used both to
	// verify the download and to name the worker pool that serves it.
	RuntimeManifestSHA256 string `json:"runtime_manifest_sha256,omitempty"`
	// RuntimeManifestDBTVersion is the dbt-core version that produced the
	// artifact. A partial parse is only loadable by the version that wrote it.
	RuntimeManifestDBTVersion string `json:"runtime_manifest_dbt_version,omitempty"`
	// RuntimeManifestParseContextSHA256 digests the parse context (command
	// dialect plus target/environment) the artifact was produced under. A
	// consumer whose own context hashes differently must not reuse it.
	RuntimeManifestParseContextSHA256 string `json:"runtime_manifest_parse_context_sha256,omitempty"`
}

// Complete reports whether every field of the reference is populated.
func (r RuntimeManifestRef) Complete() bool {
	return r.RuntimeManifestURI != "" &&
		r.RuntimeManifestSHA256 != "" &&
		r.RuntimeManifestDBTVersion != "" &&
		r.RuntimeManifestParseContextSHA256 != ""
}

// Validate accepts the zero reference (meaning "no runtime manifest") and any
// complete, well-formed one. Anything in between is rejected so a half-filled
// reference cannot silently reach an executor.
func (r RuntimeManifestRef) Validate() error {
	if r == (RuntimeManifestRef{}) {
		return nil
	}
	if !r.Complete() {
		return fmt.Errorf("runtime manifest reference is partial")
	}
	if !strings.HasPrefix(r.RuntimeManifestURI, "s3://") {
		return fmt.Errorf("runtime_manifest_uri must be s3://")
	}
	digests := []struct {
		name  string
		value string
	}{
		{"runtime_manifest_sha256", r.RuntimeManifestSHA256},
		{"runtime_manifest_parse_context_sha256", r.RuntimeManifestParseContextSHA256},
	}
	for _, d := range digests {
		if err := validateSHA256Hex(d.name, d.value); err != nil {
			return err
		}
	}
	return nil
}

// validateSHA256Hex requires exactly 64 lowercase hex characters. Digests are
// compared verbatim across services, so an uppercase or truncated spelling of
// the same value would read as a different artifact.
func validateSHA256Hex(name, value string) error {
	if len(value) != 64 || strings.ToLower(value) != value {
		return fmt.Errorf("%s must be lowercase SHA-256 hex", name)
	}
	if _, err := hex.DecodeString(value); err != nil {
		return fmt.Errorf("%s must be lowercase SHA-256 hex: %w", name, err)
	}
	return nil
}

// RuntimeManifestDescriptor is the full record of a published runtime manifest
// artifact: what it is, which release and image produced it, and the context it
// was parsed under. Producers publish the descriptor; consumers carry the
// narrower RuntimeManifestRef.
type RuntimeManifestDescriptor struct {
	Format             string `json:"format"`
	ServiceName        string `json:"service_name"`
	ReleaseID          string `json:"release_id"`
	ImageTag           string `json:"image_tag"`
	ArtifactURI        string `json:"artifact_uri"`
	SHA256             string `json:"sha256"`
	DBTCoreVersion     string `json:"dbt_core_version"`
	AdapterType        string `json:"adapter_type"`
	ParseContextSHA256 string `json:"parse_context_sha256"`
}

// Ref narrows the descriptor to the reference that travels on events.
func (d RuntimeManifestDescriptor) Ref() RuntimeManifestRef {
	return RuntimeManifestRef{
		RuntimeManifestURI:                d.ArtifactURI,
		RuntimeManifestSHA256:             d.SHA256,
		RuntimeManifestDBTVersion:         d.DBTCoreVersion,
		RuntimeManifestParseContextSHA256: d.ParseContextSHA256,
	}
}

// WorkerPoolKey names the pool of reusable workers that can serve a node: the
// workers sharing a service, a team image, and a runtime manifest are
// interchangeable, and any difference in those three needs a distinct pool. The
// NUL separator keeps the fields unambiguous, so no two different triples can
// concatenate onto the same key.
func WorkerPoolKey(serviceName, imageTag, manifestSHA string) string {
	sum := sha256.Sum256([]byte(serviceName + "\x00" + imageTag + "\x00" + manifestSHA))
	return hex.EncodeToString(sum[:])
}
