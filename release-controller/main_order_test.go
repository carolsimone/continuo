package main

import (
	"os"
	"strings"
	"testing"
)

// A run left received while no queue event fired must not wait for the next
// one after a restart: a candidate held while maintenance mode was on
// activates only when something advances the queue, and the restart that
// turns maintenance off is the first chance to. main.go therefore advances the
// queue once before it starts the stream consumers.
func TestStartupAdvancesTheQueueBeforeTheConsumersStart(t *testing.T) {
	src, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatalf("read main.go: %v", err)
	}
	body := string(src)
	advance := strings.Index(body, "handlers.AdvanceQueue(")
	consumers := strings.Index(body, `runConsumer("`)
	if advance < 0 || consumers < 0 {
		t.Fatalf("expected main.go to advance the queue and start the consumers (advance=%d consumers=%d)", advance, consumers)
	}
	if advance > consumers {
		t.Fatal("main.go must call handlers.AdvanceQueue before its first runConsumer: " +
			"a candidate held while maintenance mode was on would otherwise wait for the next queue event")
	}
}

// release-controller refuses to start on a release database that still has the
// legacy topology columns, so main.go checks the schema before it serves a
// request, runs a startup step or starts a consumer.
func TestStartupChecksTheReleaseSchemaFirst(t *testing.T) {
	src, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatalf("read main.go: %v", err)
	}
	body := string(src)
	check := strings.Index(body, "handlers.CheckLegacyTopologyColumnsDropped(")
	if check < 0 {
		t.Fatal("main.go must call handlers.CheckLegacyTopologyColumnsDropped")
	}
	for _, later := range []string{"objectstore.New(", "httpinfra.NewServer(", "redisadapter.NewClient(", "handlers.AdvanceQueue(", `runConsumer("`} {
		at := strings.Index(body, later)
		if at < 0 {
			t.Fatalf("expected main.go to contain %q", later)
		}
		if check > at {
			t.Fatalf("main.go must call handlers.CheckLegacyTopologyColumnsDropped before %s", later)
		}
	}
}
