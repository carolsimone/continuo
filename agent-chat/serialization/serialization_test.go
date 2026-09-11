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

// TestEncodeGoldenBytes pins the exact JSON bytes Encode writes for each content
// variant. These are the bytes stored in the messages.content JSONB column and
// the S3 archive, so a drift here would break deserialization of persisted
// threads.
func TestEncodeGoldenBytes(t *testing.T) {
	cases := []struct {
		name    string
		content domain.Content
		golden  string
	}{
		{"text", domain.TextContent{Text: "hi"}, goldenText},
		{"tool_call", domain.ToolCallContent{CallID: "c1", Tool: "run_sql", Args: map[string]string{"query": "select 1"}}, goldenToolCall},
		{"tool_result", domain.ToolResultContent{CallID: "c1", Output: "ok", IsError: false}, goldenToolResult},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			out, err := Encode(c.content)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			if string(out) != c.golden {
				t.Fatalf("bytes changed:\n got %s\nwant %s", out, c.golden)
			}
		})
	}
}

// TestDecodeByRoleRoundTrip checks that Decode reverses Encode for every role,
// including the two roles (user and assistant) that share the TextContent shape.
func TestDecodeByRoleRoundTrip(t *testing.T) {
	cases := []struct {
		role    domain.Role
		content domain.Content
	}{
		{domain.RoleUser, domain.TextContent{Text: "hi"}},
		{domain.RoleAssistant, domain.TextContent{Text: "hi"}},
		{domain.RoleToolCall, domain.ToolCallContent{CallID: "c1", Tool: "run_sql", Args: map[string]string{"query": "select 1"}}},
		{domain.RoleToolResult, domain.ToolResultContent{CallID: "c1", Output: "ok", IsError: true}},
	}
	for _, c := range cases {
		t.Run(string(c.role), func(t *testing.T) {
			data, err := Encode(c.content)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			got, err := Decode(c.role, data)
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			if !reflect.DeepEqual(got, c.content) {
				t.Fatalf("round-trip mismatch: got %#v want %#v", got, c.content)
			}
		})
	}
}

// TestEncodeNilContentErrors ensures a nil Content is rejected rather than
// silently written as null. The Content interface is sealed, so a nil interface
// value is the only content Encode's switch cannot match to a variant.
func TestEncodeNilContentErrors(t *testing.T) {
	if _, err := Encode(nil); err == nil {
		t.Fatal("expected error encoding a nil content")
	}
}

// TestDecodeUnknownRoleErrors ensures an unrecognized role is rejected rather
// than decoded into the wrong variant.
func TestDecodeUnknownRoleErrors(t *testing.T) {
	if _, err := Decode(domain.Role("bogus"), []byte(`{}`)); err == nil {
		t.Fatal("expected error decoding content for an unknown role")
	}
}
