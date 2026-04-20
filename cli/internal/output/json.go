package output

import (
	"encoding/json"
	"io"
)

// EmitSuccess writes a single success payload as a newline-terminated JSON object.
func EmitSuccess(w io.Writer, payload any) error {
	enc := json.NewEncoder(w)
	return enc.Encode(payload)
}

// EmitError writes a single error envelope as a newline-terminated JSON object under "error".
func EmitError(w io.Writer, e CLIError) error {
	enc := json.NewEncoder(w)
	return enc.Encode(map[string]CLIError{"error": e})
}
