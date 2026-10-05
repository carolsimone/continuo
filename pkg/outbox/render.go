package outbox

import (
	"encoding"
	"fmt"
	"strconv"
	"time"
)

// Renderer is an optional capability of a Publisher: it returns the field map
// the publisher would XADD for entry, without outbox_entry_id. The processor
// embeds those fields in the entry's dead letter so the event can be redriven
// later. A row that is not published as stream fields (for example a delay-queue
// row) returns an error and its dead letter is not redrivable.
type Renderer interface {
	Render(entry *Entry) (map[string]any, error)
}

// StringifyFields encodes XADD values as the strings Redis stores, matching
// go-redis: strings and byte slices verbatim, integers in base 10, floats in
// the shortest form of their float64 value (a float32 is widened first),
// booleans as "1"/"0", nil as "", time.Time as RFC 3339 with nanoseconds, and
// encoding.BinaryMarshaler through MarshalBinary. Any other type is an error,
// as it is for XADD.
func StringifyFields(values map[string]any) (map[string]string, error) {
	out := make(map[string]string, len(values))
	for k, v := range values {
		s, err := stringifyValue(v)
		if err != nil {
			return nil, fmt.Errorf("field %q: %w", k, err)
		}
		out[k] = s
	}
	return out, nil
}

func stringifyValue(v any) (string, error) {
	switch x := v.(type) {
	case nil:
		return "", nil
	case string:
		return x, nil
	case []byte:
		return string(x), nil
	case int:
		return strconv.FormatInt(int64(x), 10), nil
	case int8:
		return strconv.FormatInt(int64(x), 10), nil
	case int16:
		return strconv.FormatInt(int64(x), 10), nil
	case int32:
		return strconv.FormatInt(int64(x), 10), nil
	case int64:
		return strconv.FormatInt(x, 10), nil
	case uint:
		return strconv.FormatUint(uint64(x), 10), nil
	case uint8:
		return strconv.FormatUint(uint64(x), 10), nil
	case uint16:
		return strconv.FormatUint(uint64(x), 10), nil
	case uint32:
		return strconv.FormatUint(uint64(x), 10), nil
	case uint64:
		return strconv.FormatUint(x, 10), nil
	case float32:
		return strconv.FormatFloat(float64(x), 'f', -1, 64), nil
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64), nil
	case bool:
		if x {
			return "1", nil
		}
		return "0", nil
	case time.Time:
		return x.Format(time.RFC3339Nano), nil
	case encoding.BinaryMarshaler:
		b, err := x.MarshalBinary()
		if err != nil {
			return "", err
		}
		return string(b), nil
	default:
		return "", fmt.Errorf("unsupported XADD value type %T", v)
	}
}
