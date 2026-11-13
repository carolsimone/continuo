package cli_test

import (
	"os"
	"regexp"
	"testing"
)

var goPackage = regexp.MustCompile(`(?m)^option go_package = .*$`)

// TestVendoredDeadLetterProtoMatchesService keeps cli/proto/deadletter in step
// with the service's public contract; only go_package may differ.
func TestVendoredDeadLetterProtoMatchesService(t *testing.T) {
	vendored, err := os.ReadFile("proto/deadletter/v1/deadletter.proto")
	if err != nil {
		t.Fatal(err)
	}
	source, err := os.ReadFile("../dead-letter-controller/proto/deadletter/v1/deadletter.proto")
	if err != nil {
		t.Skipf("service source not present (cli built alone): %v", err)
	}
	if goPackage.ReplaceAllString(string(vendored), "") != goPackage.ReplaceAllString(string(source), "") {
		t.Fatal("cli/proto/deadletter/v1/deadletter.proto drifted from dead-letter-controller's; recopy it and run make -C cli proto")
	}
}

// TestVendoredReleaseProtoMatchesService keeps cli/proto/release in step with
// the service's public contract; only go_package may differ.
func TestVendoredReleaseProtoMatchesService(t *testing.T) {
	vendored, err := os.ReadFile("proto/release/v1/release.proto")
	if err != nil {
		t.Fatal(err)
	}
	source, err := os.ReadFile("../release-controller/proto/release/v1/release.proto")
	if err != nil {
		t.Skipf("service source not present (cli built alone): %v", err)
	}
	if goPackage.ReplaceAllString(string(vendored), "") != goPackage.ReplaceAllString(string(source), "") {
		t.Fatal("cli/proto/release/v1/release.proto drifted from release-controller's; recopy it and run make -C cli proto")
	}
}
