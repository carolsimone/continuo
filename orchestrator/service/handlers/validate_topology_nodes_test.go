package handlers

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	domainCmd "github.com/carolsimone/continuo/orchestrator/domain/command"
	"github.com/carolsimone/continuo/pkg/events"
)

func TestValidateTopologyNodes_AllPopulated_ReturnsNil(t *testing.T) {
	nodes := []domainCmd.TopologyNodePayload{
		{ServiceName: "svc-a", SchemaName: "raw", TableName: "users", ImageTag: "abc123"},
		{ServiceName: "svc-b", SchemaName: "raw", TableName: "orders", ImageTag: "def456"},
	}
	if err := validateTopologyNodes(nodes); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestValidateTopologyNodes_EmptyInput_ReturnsNil(t *testing.T) {
	if err := validateTopologyNodes(nil); err != nil {
		t.Fatalf("expected nil for nil input, got %v", err)
	}
}

func TestValidateTopologyNodes_SingleEmpty_ReturnsWrappedPermanent(t *testing.T) {
	nodes := []domainCmd.TopologyNodePayload{
		{ServiceName: "svc-a", SchemaName: "raw", TableName: "users", ImageTag: ""},
	}
	err := validateTopologyNodes(nodes)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, events.ErrPermanent) {
		t.Fatalf("expected wrapped ErrPermanent, got %v", err)
	}
	if !strings.Contains(err.Error(), "svc-a/raw/users") {
		t.Fatalf("expected offender triple in message, got %q", err.Error())
	}
}

func TestValidateTopologyNodes_MultipleEmpty_AggregatedAndSorted(t *testing.T) {
	nodes := []domainCmd.TopologyNodePayload{
		{ServiceName: "z-svc", SchemaName: "raw", TableName: "z-tbl", ImageTag: ""},
		{ServiceName: "a-svc", SchemaName: "raw", TableName: "a-tbl", ImageTag: ""},
		{ServiceName: "m-svc", SchemaName: "raw", TableName: "m-tbl", ImageTag: ""},
	}
	err := validateTopologyNodes(nodes)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	msg := err.Error()
	idxA := strings.Index(msg, "a-svc/raw/a-tbl")
	idxM := strings.Index(msg, "m-svc/raw/m-tbl")
	idxZ := strings.Index(msg, "z-svc/raw/z-tbl")
	if !(idxA >= 0 && idxA < idxM && idxM < idxZ) {
		t.Fatalf("expected sorted order a-svc < m-svc < z-svc, got %q", msg)
	}
}

func TestValidateTopologyNodes_OverTen_Truncated(t *testing.T) {
	nodes := make([]domainCmd.TopologyNodePayload, 12)
	for i := range nodes {
		nodes[i] = domainCmd.TopologyNodePayload{
			ServiceName: fmt.Sprintf("svc-%02d", i),
			SchemaName:  "raw",
			TableName:   fmt.Sprintf("tbl-%02d", i),
			ImageTag:    "",
		}
	}
	err := validateTopologyNodes(nodes)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	msg := err.Error()
	if !strings.Contains(msg, "...and 2 more") {
		t.Fatalf("expected truncation marker '...and 2 more', got %q", msg)
	}
	// Lex-sorted: svc-00..svc-09 fit in the first 10 slots; svc-10 and svc-11
	// are truncated and must not appear in the detail list.
	if strings.Contains(msg, "svc-10/raw/tbl-10") {
		t.Fatalf("svc-10 should be truncated, got %q", msg)
	}
	if strings.Contains(msg, "svc-11/raw/tbl-11") {
		t.Fatalf("svc-11 should be truncated, got %q", msg)
	}
}

func TestValidateTopologyNodes_ExactlyTen_NoTruncation(t *testing.T) {
	nodes := make([]domainCmd.TopologyNodePayload, 10)
	for i := range nodes {
		nodes[i] = domainCmd.TopologyNodePayload{
			ServiceName: fmt.Sprintf("svc-%02d", i),
			SchemaName:  "raw",
			TableName:   fmt.Sprintf("tbl-%02d", i),
			ImageTag:    "",
		}
	}
	err := validateTopologyNodes(nodes)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if strings.Contains(err.Error(), "more") {
		t.Fatalf("exactly-10 must NOT include truncation marker, got %q", err.Error())
	}
}
