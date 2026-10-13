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
