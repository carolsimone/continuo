package s3

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseS3URI(t *testing.T) {
	for _, tc := range []struct{ in, bucket, key string }{
		{"s3://continuo/code-bundles/rel-1/bundle.json", "continuo", "code-bundles/rel-1/bundle.json"},
		{"code-bundles/rel-1/bundle.json", "", "code-bundles/rel-1/bundle.json"},
		{"s3://continuo", "continuo", ""},
	} {
		b, k := parseS3URI(tc.in)
		assert.Equal(t, tc.bucket, b, tc.in)
		assert.Equal(t, tc.key, k, tc.in)
	}
}
