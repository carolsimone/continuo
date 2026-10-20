package main

import (
	"os"
	"strings"
	"testing"
)

// execution-controller refuses to start on an execution database that lacks a
// column its code reads, so main.go checks the schema before it waits for
// Redis, builds a Kubernetes client or starts any consumer.
func TestStartupChecksTheExecutionSchemaFirst(t *testing.T) {
	src, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatalf("read main.go: %v", err)
	}
	body := string(src)
	check := strings.Index(body, "handlers.CheckSchema(")
	if check < 0 {
		t.Fatal("main.go must call handlers.CheckSchema")
	}
	for _, later := range []string{"pkgredis.WaitForRedis(", "k8s.NewK8sClient(", "objectstore.New(", `runConsumer("`} {
		at := strings.Index(body, later)
		if at < 0 {
			t.Fatalf("expected main.go to contain %q", later)
		}
		if check > at {
			t.Fatalf("main.go must call handlers.CheckSchema before %s", later)
		}
	}
}
