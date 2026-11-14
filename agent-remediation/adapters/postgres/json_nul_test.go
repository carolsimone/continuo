package postgres

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReplaceNULEscapes(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{name: "a NUL escape is replaced", in: `{"a":"x\u0000y"}`, want: `{"a":"x\ufffdy"}`},
		{name: "every NUL escape is replaced", in: `["\u0000","\u0000\u0000"]`, want: `["\ufffd","\ufffd\ufffd"]`},
		{name: "an escaped backslash before u0000 is kept", in: `{"a":"x\\u0000y"}`, want: `{"a":"x\\u0000y"}`},
		{name: "an escaped backslash then a NUL escape", in: `{"a":"x\\\u0000y"}`, want: `{"a":"x\\\ufffdy"}`},
		{name: "two escaped backslashes before u0000 are kept", in: `{"a":"\\\\u0000"}`, want: `{"a":"\\\\u0000"}`},
		{name: "another escape is kept", in: `{"a":"\u00001\u0001\n"}`, want: `{"a":"\ufffd1\u0001\n"}`},
		{name: "a key holding a NUL escape", in: `{"k\u0000":1}`, want: `{"k\ufffd":1}`},
		{name: "JSON with no NUL is unchanged", in: `{"a":"select 1","b":[1,2]}`, want: `{"a":"select 1","b":[1,2]}`},
		{name: "a trailing backslash is kept", in: `\`, want: `\`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, string(replaceNULEscapes([]byte(tc.in))))
		})
	}
}

func TestReplaceNULEscapes_DecodesToTheReplacementCharacter(t *testing.T) {
	raw, err := json.Marshal(map[string]string{"source": "select 1\x00", "path": `C:\u0000`})
	require.NoError(t, err)
	var got map[string]string
	require.NoError(t, json.Unmarshal(replaceNULEscapes(raw), &got))
	assert.Equal(t, "select 1\uFFFD", got["source"])
	assert.Equal(t, `C:\u0000`, got["path"], "a backslash the text holds is not an escape")
}
