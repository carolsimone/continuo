package model

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRuntimeManifestRefEmpty(t *testing.T) {
	var ref RuntimeManifestRef
	require.NoError(t, ref.Validate(), "the zero reference means 'no runtime manifest' and is valid")
	assert.False(t, ref.Complete())
}

func TestRuntimeManifestRefPartialIsInvalid(t *testing.T) {
	partial := RuntimeManifestRef{
		RuntimeManifestURI:    "s3://continuo/finance/r1/partial_parse.msgpack",
		RuntimeManifestSHA256: strings.Repeat("a", 64),
	}
	assert.False(t, partial.Complete())
	require.ErrorContains(t, partial.Validate(), "partial")
}

func TestRuntimeManifestRefComplete(t *testing.T) {
	ref := RuntimeManifestRef{
		RuntimeManifestURI:                "s3://continuo/finance/r1/partial_parse.msgpack",
		RuntimeManifestSHA256:             strings.Repeat("a", 64),
		RuntimeManifestDBTVersion:         "1.12.0b1",
		RuntimeManifestParseContextSHA256: strings.Repeat("b", 64),
	}
	require.NoError(t, ref.Validate())
	assert.True(t, ref.Complete())
	assert.Equal(t,
		"2f05cf2ba42b4ecf8d92dc00a11a0c706e8164af241ef43c21fb2bd6c2e0814b",
		WorkerPoolKey("finance", "sha-123", ref.RuntimeManifestSHA256))
}

func TestRuntimeManifestRefRejectsNonS3URI(t *testing.T) {
	ref := RuntimeManifestRef{
		RuntimeManifestURI:                "https://continuo/finance/r1/partial_parse.msgpack",
		RuntimeManifestSHA256:             strings.Repeat("a", 64),
		RuntimeManifestDBTVersion:         "1.12.0b1",
		RuntimeManifestParseContextSHA256: strings.Repeat("b", 64),
	}
	require.ErrorContains(t, ref.Validate(), "s3://")
}

// The digest fields are exchanged across services and compared verbatim, so
// only exact lowercase 64-char hex is accepted: uppercase, short, long, and
// non-hex values must all be rejected.
func TestRuntimeManifestRefRejectsMalformedDigests(t *testing.T) {
	cases := map[string]string{
		"uppercase": strings.Repeat("A", 64),
		"too short": strings.Repeat("a", 63),
		"too long":  strings.Repeat("a", 65),
		"non-hex":   strings.Repeat("z", 64),
	}
	for name, bad := range cases {
		t.Run("manifest sha "+name, func(t *testing.T) {
			ref := RuntimeManifestRef{
				RuntimeManifestURI:                "s3://continuo/finance/r1/partial_parse.msgpack",
				RuntimeManifestSHA256:             bad,
				RuntimeManifestDBTVersion:         "1.12.0b1",
				RuntimeManifestParseContextSHA256: strings.Repeat("b", 64),
			}
			require.ErrorContains(t, ref.Validate(), "runtime_manifest_sha256 must be lowercase SHA-256 hex")
		})
		t.Run("parse context sha "+name, func(t *testing.T) {
			ref := RuntimeManifestRef{
				RuntimeManifestURI:                "s3://continuo/finance/r1/partial_parse.msgpack",
				RuntimeManifestSHA256:             strings.Repeat("a", 64),
				RuntimeManifestDBTVersion:         "1.12.0b1",
				RuntimeManifestParseContextSHA256: bad,
			}
			require.ErrorContains(t, ref.Validate(), "runtime_manifest_parse_context_sha256 must be lowercase SHA-256 hex")
		})
	}
}

// The ref travels over Redis Streams as JSON; an absent reference must
// serialize to an empty object so consumers on the old contract see no fields.
func TestRuntimeManifestRefJSONTags(t *testing.T) {
	empty, err := json.Marshal(RuntimeManifestRef{})
	require.NoError(t, err)
	assert.JSONEq(t, `{}`, string(empty))

	body, err := json.Marshal(RuntimeManifestRef{
		RuntimeManifestURI:                "s3://continuo/finance/r1/partial_parse.msgpack",
		RuntimeManifestSHA256:             strings.Repeat("a", 64),
		RuntimeManifestDBTVersion:         "1.12.0b1",
		RuntimeManifestParseContextSHA256: strings.Repeat("b", 64),
	})
	require.NoError(t, err)
	assert.JSONEq(t, `{
		"runtime_manifest_uri": "s3://continuo/finance/r1/partial_parse.msgpack",
		"runtime_manifest_sha256": "`+strings.Repeat("a", 64)+`",
		"runtime_manifest_dbt_version": "1.12.0b1",
		"runtime_manifest_parse_context_sha256": "`+strings.Repeat("b", 64)+`"
	}`, string(body))
}

func TestRuntimeManifestDescriptorRef(t *testing.T) {
	d := RuntimeManifestDescriptor{
		Format:             RuntimeManifestFormatV1,
		ServiceName:        "finance",
		ReleaseID:          "r1",
		ImageTag:           "sha-123",
		ArtifactURI:        "s3://continuo/finance/r1/partial_parse.msgpack",
		SHA256:             strings.Repeat("a", 64),
		DBTCoreVersion:     "1.12.0b1",
		AdapterType:        "postgres",
		ParseContextSHA256: strings.Repeat("b", 64),
	}
	ref := d.Ref()
	require.NoError(t, ref.Validate())
	assert.Equal(t, RuntimeManifestRef{
		RuntimeManifestURI:                "s3://continuo/finance/r1/partial_parse.msgpack",
		RuntimeManifestSHA256:             strings.Repeat("a", 64),
		RuntimeManifestDBTVersion:         "1.12.0b1",
		RuntimeManifestParseContextSHA256: strings.Repeat("b", 64),
	}, ref)
}

// The pool key names a reusable worker pool, so it must be stable across
// processes and must change whenever any of its three inputs changes.
func TestWorkerPoolKeyDeterministicAndDistinct(t *testing.T) {
	sha := strings.Repeat("a", 64)
	base := WorkerPoolKey("finance", "sha-123", sha)
	assert.Equal(t, base, WorkerPoolKey("finance", "sha-123", sha), "same inputs must give the same key")
	assert.Len(t, base, 64)

	assert.NotEqual(t, base, WorkerPoolKey("risk", "sha-123", sha), "service name is part of the key")
	assert.NotEqual(t, base, WorkerPoolKey("finance", "sha-456", sha), "image tag is part of the key")
	assert.NotEqual(t, base, WorkerPoolKey("finance", "sha-123", strings.Repeat("b", 64)), "manifest sha is part of the key")
}

// The NUL separator keeps the three fields unambiguous: no regrouping of the
// same concatenated characters may collide onto one key.
func TestWorkerPoolKeySeparatorPreventsCollision(t *testing.T) {
	assert.NotEqual(t,
		WorkerPoolKey("fin", "ance", strings.Repeat("a", 64)),
		WorkerPoolKey("finance", "", strings.Repeat("a", 64)))
}
