package exportfile

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/carolsimone/continuo/catalog/service/export"
)

// A property value is written as follows, and read back as the same Go value:
//
//   - a string, a bool and null are plain JSON, and a list a JSON array of
//     values written the same way;
//   - an int64 is {"$int":"<decimal>"}, so no JSON reader rounds it;
//   - a float64 is {"$float":"<shortest text that reads back exactly>"},
//     with NaN, +Inf and -Inf as text and -0 kept;
//   - a time.Time is {"$datetime":"<RFC 3339 text>"}, with nanoseconds. A
//     datetime Neo4j stores with a fixed offset is written in that offset
//     ("Z" for UTC); one it stores with a zone name carries the name in
//     brackets, as RFC 9557 does ("2026-10-30T02:00:00Z[UTC]"). Neo4j holds
//     those two as different values, so the name is kept.
//
// The Neo4j driver hands a datetime stored with a fixed offset over in a
// location named Offset (time.FixedZone("Offset", seconds)) and sends one in
// that location back with its offset; a datetime in any other location goes
// to Neo4j by its location's name. Reading a datetime gives the location back
// the same way.
//
// Every other Go type, and a plain JSON number, is ErrUnsupportedValue on
// both sides. A reader accepts each typed value only in the text the writer
// writes for it.
const (
	intTag      = "$int"
	floatTag    = "$float"
	datetimeTag = "$datetime"
	offsetZone  = "Offset"
)

// encodeValue returns v in its JSON shape.
func encodeValue(v any) (any, error) {
	switch x := v.(type) {
	case nil:
		return nil, nil
	case string:
		if !utf8.ValidString(x) {
			return nil, fmt.Errorf("%w: a string that is not UTF-8", export.ErrUnsupportedValue)
		}
		return x, nil
	case bool:
		return x, nil
	case int64:
		return map[string]string{intTag: strconv.FormatInt(x, 10)}, nil
	case float64:
		return map[string]string{floatTag: strconv.FormatFloat(x, 'g', -1, 64)}, nil
	case time.Time:
		text, err := formatDateTime(x)
		if err != nil {
			return nil, err
		}
		return map[string]string{datetimeTag: text}, nil
	case []any:
		out := make([]any, 0, len(x))
		for i, e := range x {
			enc, err := encodeValue(e)
			if err != nil {
				return nil, fmt.Errorf("list element %d: %w", i, err)
			}
			out = append(out, enc)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("%w: a value of type %T", export.ErrUnsupportedValue, v)
	}
}

// formatDateTime writes t as RFC 3339 text in t's own offset, followed by
// its zone name in brackets unless t is in the driver's offset location. It
// refuses a time it could not read back as the same instant, offset and
// location: the Local zone, an unnamed or unknown zone, an offset in
// seconds, a year outside 0000-9999.
func formatDateTime(t time.Time) (string, error) {
	text := t.Format(time.RFC3339Nano)
	if name := t.Location().String(); name != offsetZone {
		text += "[" + name + "]"
	}
	back, err := parseDateTime(text)
	if err != nil {
		return "", fmt.Errorf("datetime %v: %w", t, err)
	}
	_, off := t.Zone()
	_, backOff := back.Zone()
	if !back.Equal(t) || backOff != off || back.Location().String() != t.Location().String() {
		return "", fmt.Errorf("%w: datetime %v cannot be written exactly", export.ErrUnsupportedValue, t)
	}
	return text, nil
}

// parseDateTime reads what formatDateTime writes. Text without a zone name
// comes back in the driver's offset location; text with one in that zone,
// whose offset at that instant must be the text's.
func parseDateTime(s string) (time.Time, error) {
	text, name, named := s, "", false
	if strings.HasSuffix(s, "]") {
		i := strings.LastIndexByte(s, '[')
		if i < 0 {
			return time.Time{}, fmt.Errorf("%w: datetime %q", export.ErrUnsupportedValue, s)
		}
		text, name, named = s[:i], s[i+1:len(s)-1], true
	}
	t, err := time.Parse(time.RFC3339Nano, text)
	if err != nil {
		return time.Time{}, fmt.Errorf("%w: datetime %q: %v", export.ErrUnsupportedValue, s, err)
	}
	_, off := t.Zone()
	if !named {
		return t.In(time.FixedZone(offsetZone, off)), nil
	}
	if name == "" || name == "Local" || name == offsetZone {
		return time.Time{}, fmt.Errorf("%w: datetime %q names no zone Neo4j knows", export.ErrUnsupportedValue, s)
	}
	loc, err := time.LoadLocation(name)
	if err != nil {
		return time.Time{}, fmt.Errorf("%w: datetime %q: %v", export.ErrUnsupportedValue, s, err)
	}
	in := t.In(loc)
	if _, zoneOff := in.Zone(); zoneOff != off {
		return time.Time{}, fmt.Errorf("%w: datetime %q: the offset is not %s's at that time", export.ErrUnsupportedValue, s, name)
	}
	return in, nil
}

// decodeValue reads one value written by encodeValue. raw holds exactly one
// JSON value.
func decodeValue(raw []byte) (any, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return nil, fmt.Errorf("%w: an empty value", ErrMalformed)
	}
	switch c := trimmed[0]; {
	case c == '"' || c == 't' || c == 'f':
		var v any
		if err := json.Unmarshal(trimmed, &v); err != nil {
			return nil, fmt.Errorf("%w: %v", ErrMalformed, err)
		}
		return v, nil
	case c == 'n':
		if string(trimmed) != "null" {
			return nil, fmt.Errorf("%w: %q is not a value", ErrMalformed, trimmed)
		}
		return nil, nil
	case c == '[':
		var elems []json.RawMessage
		if err := json.Unmarshal(trimmed, &elems); err != nil {
			return nil, fmt.Errorf("%w: %v", ErrMalformed, err)
		}
		out := make([]any, 0, len(elems))
		for i, e := range elems {
			v, err := decodeValue(e)
			if err != nil {
				return nil, fmt.Errorf("list element %d: %w", i, err)
			}
			out = append(out, v)
		}
		return out, nil
	case c == '{':
		return decodeTyped(trimmed)
	case c == '-' || c >= '0' && c <= '9':
		return nil, fmt.Errorf("%w: a plain JSON number %s; an integer is {%q:\"…\"} and a float {%q:\"…\"}",
			export.ErrUnsupportedValue, trimmed, intTag, floatTag)
	default:
		return nil, fmt.Errorf("%w: %q is not a value", ErrMalformed, trimmed)
	}
}

// decodeTyped reads {"$int"|"$float"|"$datetime": "<text>"}, accepting the
// text only in the form the writer writes.
func decodeTyped(raw []byte) (any, error) {
	members, err := decodeObject(raw)
	if err != nil {
		return nil, err
	}
	if len(members) != 1 {
		return nil, fmt.Errorf("%w: an object value must have one member %s, %s or %s", export.ErrUnsupportedValue, intTag, floatTag, datetimeTag)
	}
	tag := members[0].name
	var text string
	if err := json.Unmarshal(members[0].value, &text); err != nil {
		return nil, fmt.Errorf("%w: %s holds %s, want a JSON string", export.ErrUnsupportedValue, tag, members[0].value)
	}
	var v any
	var canonical string
	switch tag {
	case intTag:
		i, err := strconv.ParseInt(text, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("%w: %s %q: %v", export.ErrUnsupportedValue, tag, text, err)
		}
		v, canonical = i, strconv.FormatInt(i, 10)
	case floatTag:
		f, err := strconv.ParseFloat(text, 64)
		if err != nil {
			return nil, fmt.Errorf("%w: %s %q: %v", export.ErrUnsupportedValue, tag, text, err)
		}
		v, canonical = f, strconv.FormatFloat(f, 'g', -1, 64)
	case datetimeTag:
		t, err := parseDateTime(text)
		if err != nil {
			return nil, err
		}
		if canonical, err = formatDateTime(t); err != nil {
			return nil, err
		}
		v = t
	default:
		return nil, fmt.Errorf("%w: unknown typed value %q", export.ErrUnsupportedValue, tag)
	}
	if text != canonical {
		return nil, fmt.Errorf("%w: %s %q is written %q", export.ErrUnsupportedValue, tag, text, canonical)
	}
	return v, nil
}

// member is one name and raw value of a JSON object, in the object's order.
type member struct {
	name  string
	value json.RawMessage
}

// decodeObject reads raw, which must hold one JSON object and nothing after
// it, and returns its members. A name given twice is ErrMalformed, so no
// reader takes the first and another the last.
func decodeObject(raw []byte) ([]member, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	if tok, err := dec.Token(); err != nil || tok != json.Delim('{') {
		return nil, fmt.Errorf("%w: not a JSON object", ErrMalformed)
	}
	var out []member
	seen := map[string]bool{}
	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrMalformed, err)
		}
		name, ok := tok.(string)
		if !ok {
			return nil, fmt.Errorf("%w: a member without a name", ErrMalformed)
		}
		if seen[name] {
			return nil, fmt.Errorf("%w: member %q twice", ErrMalformed, name)
		}
		seen[name] = true
		var value json.RawMessage
		if err := dec.Decode(&value); err != nil {
			return nil, fmt.Errorf("%w: member %q: %v", ErrMalformed, name, err)
		}
		out = append(out, member{name: name, value: value})
	}
	if tok, err := dec.Token(); err != nil || tok != json.Delim('}') {
		return nil, fmt.Errorf("%w: an object that does not close", ErrMalformed)
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("%w: data after the object", ErrMalformed)
	}
	return out, nil
}
