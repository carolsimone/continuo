package config

import (
	"os"
	"path/filepath"
	"testing"
)

// clearS3Keys unsets both S3 keys in both forms.
func clearS3Keys(t *testing.T) {
	t.Helper()
	for _, key := range []string{
		"AWS_ACCESS_KEY_ID", "AWS_ACCESS_KEY_ID_FILE",
		"AWS_SECRET_ACCESS_KEY", "AWS_SECRET_ACCESS_KEY_FILE",
	} {
		t.Setenv(key, "")
	}
}

func TestLoadS3_records_all_missing(t *testing.T) {
	for _, key := range []string{"S3_ENDPOINT_URL", "S3_BUCKET", "AWS_DEFAULT_REGION"} {
		t.Setenv(key, "")
	}
	clearS3Keys(t)
	v := &Validator{}
	LoadS3(v)
	if got := len(v.Missing()); got != 3 {
		t.Fatalf("want 3 missing vars, got %d: %v", got, v.Missing())
	}
}

// The S3 keys are optional: with neither form of either key set, LoadS3
// records no problem and returns empty keys, which leave the SDK's default
// credential chain to sign requests.
func TestLoadS3_no_missing_when_all_set(t *testing.T) {
	t.Setenv("S3_ENDPOINT_URL", "http://s3:9000")
	t.Setenv("S3_BUCKET", "mybucket")
	t.Setenv("AWS_DEFAULT_REGION", "eu-west-1")
	clearS3Keys(t)
	v := &Validator{}
	cfg := LoadS3(v)
	if len(v.Missing()) != 0 {
		t.Fatalf("want no missing, got %v", v.Missing())
	}
	if cfg.EndpointURL != "http://s3:9000" || cfg.Bucket != "mybucket" || cfg.Region != "eu-west-1" {
		t.Fatalf("unexpected config: %+v", cfg)
	}
	if cfg.AccessKeyID != "" || cfg.SecretAccessKey != "" {
		t.Fatalf("want empty keys, got id=%q secret=%q", cfg.AccessKeyID, cfg.SecretAccessKey)
	}
}

func TestLoadS3ReadsKeysFromFiles(t *testing.T) {
	dir := t.TempDir()
	idFile := filepath.Join(dir, "AWS_ACCESS_KEY_ID")
	keyFile := filepath.Join(dir, "AWS_SECRET_ACCESS_KEY")
	if err := os.WriteFile(idFile, []byte("AKIAFROMFILE\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, []byte("secret-from-file\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("S3_ENDPOINT_URL", "http://s3:9000")
	t.Setenv("S3_BUCKET", "mybucket")
	t.Setenv("AWS_DEFAULT_REGION", "eu-west-1")
	t.Setenv("AWS_ACCESS_KEY_ID", "")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "")
	t.Setenv("AWS_ACCESS_KEY_ID_FILE", idFile)
	t.Setenv("AWS_SECRET_ACCESS_KEY_FILE", keyFile)

	v := &Validator{}
	cfg := LoadS3(v)
	if len(v.Missing()) != 0 {
		t.Fatalf("want no problems, got %v", v.Missing())
	}
	if cfg.AccessKeyID != "AKIAFROMFILE" || cfg.SecretAccessKey != "secret-from-file" {
		t.Fatalf("keys not read from files: id=%q secret=%q", cfg.AccessKeyID, cfg.SecretAccessKey)
	}
}

func TestLoadS3RejectsKeyGivenTwice(t *testing.T) {
	file := filepath.Join(t.TempDir(), "AWS_ACCESS_KEY_ID")
	if err := os.WriteFile(file, []byte("AKIAFROMFILE"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("S3_ENDPOINT_URL", "http://s3:9000")
	t.Setenv("S3_BUCKET", "mybucket")
	t.Setenv("AWS_DEFAULT_REGION", "eu-west-1")
	clearS3Keys(t)
	t.Setenv("AWS_ACCESS_KEY_ID", "AKIAFROMENV")
	t.Setenv("AWS_ACCESS_KEY_ID_FILE", file)

	v := &Validator{}
	LoadS3(v)
	want := "AWS_ACCESS_KEY_ID: set AWS_ACCESS_KEY_ID or AWS_ACCESS_KEY_ID_FILE, not both"
	if got := v.Missing(); len(got) != 1 || got[0] != want {
		t.Fatalf("Missing() = %v, want [%q]", got, want)
	}
}
