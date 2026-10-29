package main

import (
	"os"
	"strings"
	"testing"
)

// run-controller refuses to start on a database that lacks a column its code
// reads, so main.go checks the schema before it waits for Redis, settles the
// active runs or starts any consumer.
func TestStartupChecksTheRunControllerSchemaFirst(t *testing.T) {
	src, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatalf("read main.go: %v", err)
	}
	body := string(src)
	check := strings.Index(body, "svchandlers.CheckSchema(")
	if check < 0 {
		t.Fatal("main.go must call svchandlers.CheckSchema")
	}
	for _, later := range []string{"pkgredis.WaitForRedis(", "runsettle.Settle(", "runConsumer(c.name"} {
		at := strings.Index(body, later)
		if at < 0 {
			t.Fatalf("expected main.go to contain %q", later)
		}
		if check > at {
			t.Fatalf("main.go must call svchandlers.CheckSchema before %s", later)
		}
	}
}
