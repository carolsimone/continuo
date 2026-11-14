package postgres

import "bytes"

// nulEscape is how JSON writes the character U+0000, and replacementEscape how
// it writes U+FFFD, the replacement character.
var (
	nulEscape         = []byte("\\u0000")
	replacementEscape = []byte("\\ufffd")
)

// replaceNULEscapes returns raw, a JSON document, with every \u0000 escape
// replaced by the replacement character's escape. A jsonb column refuses a
// string holding U+0000, and encoding/json writes a NUL byte of the text it
// encodes as \u0000. A backslash the text holds is written \\, so each
// backslash is read together with the character after it: \\u0000 is a
// backslash followed by "u0000", and is kept. raw is returned as it is when it
// holds no NUL escape.
func replaceNULEscapes(raw []byte) []byte {
	if !bytes.Contains(raw, nulEscape) {
		return raw
	}
	out := make([]byte, 0, len(raw))
	for i := 0; i < len(raw); i++ {
		if raw[i] != '\\' {
			out = append(out, raw[i])
			continue
		}
		if bytes.HasPrefix(raw[i:], nulEscape) {
			out = append(out, replacementEscape...)
			i += len(nulEscape) - 1
			continue
		}
		out = append(out, raw[i])
		if i+1 < len(raw) {
			out = append(out, raw[i+1])
			i++
		}
	}
	return out
}
