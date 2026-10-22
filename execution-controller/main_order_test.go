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

// Nothing creates or observes a Job before the Job watch's caches have synced:
// until then a Job absent from the cache may only be not listed yet, and a
// dispatch decided against it would report an attempt whose Job runs as lost.
func TestStartupWaitsForTheJobWatchBeforeItsWorkersStart(t *testing.T) {
	src, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatalf("read main.go: %v", err)
	}
	body := string(src)
	synced := strings.Index(body, "jobWatch.WaitForCacheSync(")
	if synced < 0 {
		t.Fatal("main.go must wait for jobWatch.WaitForCacheSync")
	}
	for _, later := range []string{`runWorker("deploy_dispatcher"`, `runWorker("observer"`, `runConsumer("query_model"`, `runConsumer("task_dispatch"`} {
		at := strings.Index(body, later)
		if at < 0 {
			t.Fatalf("expected main.go to contain %q", later)
		}
		if synced > at {
			t.Fatalf("main.go must wait for the Job watch to sync before %s", later)
		}
	}
}
