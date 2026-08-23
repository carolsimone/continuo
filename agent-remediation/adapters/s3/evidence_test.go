package s3

import "testing"

func TestParseS3URI(t *testing.T) {
	cases := []struct {
		in, wantBucket, wantKey string
	}{
		{"s3://mybucket/logs/a/b.log", "mybucket", "logs/a/b.log"},
		{"logs/a/b.log", "", "logs/a/b.log"}, // bare key, default bucket applies
	}
	for _, tc := range cases {
		b, k := parseS3URI(tc.in)
		if b != tc.wantBucket || k != tc.wantKey {
			t.Errorf("parseS3URI(%q) = (%q,%q), want (%q,%q)", tc.in, b, k, tc.wantBucket, tc.wantKey)
		}
	}
}
