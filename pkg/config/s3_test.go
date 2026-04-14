package config

import "testing"

func TestLoadS3_records_all_missing(t *testing.T) {
	for _, key := range []string{"S3_ENDPOINT_URL", "S3_BUCKET", "AWS_DEFAULT_REGION"} {
		t.Setenv(key, "")
	}
	v := &Validator{}
	LoadS3(v)
	if got := len(v.Missing()); got != 3 {
		t.Fatalf("want 3 missing vars, got %d: %v", got, v.Missing())
	}
}

func TestLoadS3_no_missing_when_all_set(t *testing.T) {
	t.Setenv("S3_ENDPOINT_URL", "http://s3:9000")
	t.Setenv("S3_BUCKET", "mybucket")
	t.Setenv("AWS_DEFAULT_REGION", "eu-west-1")
	v := &Validator{}
	cfg := LoadS3(v)
	if len(v.Missing()) != 0 {
		t.Fatalf("want no missing, got %v", v.Missing())
	}
	if cfg.EndpointURL != "http://s3:9000" || cfg.Bucket != "mybucket" || cfg.Region != "eu-west-1" {
		t.Fatalf("unexpected config: %+v", cfg)
	}
}
