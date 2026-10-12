package objectstore_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/pkg/objectstore"
)

func TestParseRef_AcceptsAnS3URIOrABareKey(t *testing.T) {
	for _, tc := range []struct {
		name string
		uri  string
		want objectstore.Ref
	}{
		{"s3 uri", "s3://continuo/code-bundles/rel-1/bundle.json", objectstore.Ref{Bucket: "continuo", Key: "code-bundles/rel-1/bundle.json"}},
		{"s3 uri with a flat key", "s3://logs-bucket/a.log", objectstore.Ref{Bucket: "logs-bucket", Key: "a.log"}},
		{"bare key", "code-bundles/rel-1/bundle.json", objectstore.Ref{Key: "code-bundles/rel-1/bundle.json"}},
		{"bare flat key", "a.log", objectstore.Ref{Key: "a.log"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := objectstore.ParseRef(tc.uri)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
			assert.Equal(t, tc.uri, got.String(), "String gives back the URI ParseRef read")
		})
	}
}

// A reference that names no object is refused at the boundary, before any
// request. Sent to S3, a GET of an empty key fails as an ordinary error and
// is retried until the message is dead-lettered.
func TestParseRef_RefusesAReferenceThatNamesNoObject(t *testing.T) {
	for _, tc := range []struct{ name, uri string }{
		{"empty", ""},
		{"blank", "   "},
		{"scheme only", "s3://"},
		{"no bucket", "s3:///code-bundles/rel-1/bundle.json"},
		{"bucket without a key", "s3://continuo"},
		{"bucket with an empty key", "s3://continuo/"},
		{"bare key with a leading slash", "/code-bundles/rel-1/bundle.json"},
		{"s3 key with a leading slash", "s3://continuo//k"},
		{"another scheme", "gs://continuo/k"},
		{"an https url", "https://minio:9000/continuo/k"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := objectstore.ParseRef(tc.uri)
			require.ErrorIs(t, err, objectstore.ErrInvalidRef)
			assert.NotErrorIs(t, err, objectstore.ErrNotFound)
		})
	}
}

// Validate holds a Ref built in code to the rule ParseRef applies to a URI.
func TestRef_Validate(t *testing.T) {
	for _, tc := range []struct {
		name  string
		ref   objectstore.Ref
		valid bool
	}{
		{"bare key", objectstore.Ref{Key: "k"}, true},
		{"bucket and key", objectstore.Ref{Bucket: "continuo", Key: "a/b"}, true},
		{"no key", objectstore.Ref{Bucket: "continuo"}, false},
		{"blank key", objectstore.Ref{Key: " "}, false},
		{"key with a leading slash", objectstore.Ref{Key: "/k"}, false},
		{"bucket with a slash", objectstore.Ref{Bucket: "a/b", Key: "k"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.ref.Validate()
			if tc.valid {
				assert.NoError(t, err)
				return
			}
			assert.ErrorIs(t, err, objectstore.ErrInvalidRef)
		})
	}
}

func TestRef_StringNamesTheBucketOnlyWhenSet(t *testing.T) {
	assert.Equal(t, "s3://continuo/a/b", objectstore.Ref{Bucket: "continuo", Key: "a/b"}.String())
	assert.Equal(t, "a/b", objectstore.Ref{Key: "a/b"}.String())
}
