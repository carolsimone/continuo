package serialization

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/carolsimone/continuo/agent-chat/domain"
)

const (
	goldenText       = `{"text":"hi"}`
	goldenToolCall   = `{"call_id":"c1","tool":"run_sql","args":{"query":"select 1"}}`
	goldenToolResult = `{"call_id":"c1","output":"ok","is_error":false}`
)

func TestTextContentRoundTrip(t *testing.T) {
	var dto TextContentDTO
	if err := json.Unmarshal([]byte(goldenText), &dto); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if dto.ToDomain() != (domain.TextContent{Text: "hi"}) {
		t.Fatalf("toDomain: %+v", dto.ToDomain())
	}
	out, _ := json.Marshal(TextContentFromDomain(dto.ToDomain()))
	if string(out) != goldenText {
		t.Fatalf("bytes changed:\n got %s\nwant %s", out, goldenText)
	}
}

func TestToolCallContentRoundTrip(t *testing.T) {
	var dto ToolCallContentDTO
	if err := json.Unmarshal([]byte(goldenToolCall), &dto); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	want := domain.ToolCallContent{CallID: "c1", Tool: "run_sql", Args: map[string]string{"query": "select 1"}}
	if !reflect.DeepEqual(dto.ToDomain(), want) {
		t.Fatalf("toDomain: %+v", dto.ToDomain())
	}
	out, _ := json.Marshal(ToolCallContentFromDomain(dto.ToDomain()))
	if string(out) != goldenToolCall {
		t.Fatalf("bytes changed:\n got %s\nwant %s", out, goldenToolCall)
	}
}

func TestToolResultContentRoundTrip(t *testing.T) {
	var dto ToolResultContentDTO
	if err := json.Unmarshal([]byte(goldenToolResult), &dto); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if dto.ToDomain() != (domain.ToolResultContent{CallID: "c1", Output: "ok", IsError: false}) {
		t.Fatalf("toDomain: %+v", dto.ToDomain())
	}
	out, _ := json.Marshal(ToolResultContentFromDomain(dto.ToDomain()))
	if string(out) != goldenToolResult {
		t.Fatalf("bytes changed:\n got %s\nwant %s", out, goldenToolResult)
	}
}
