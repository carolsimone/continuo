package remediation

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestKeepFileConventions(t *testing.T) {
	cases := []struct {
		name     string
		original string
		content  string
		want     string
	}{
		{
			name:     "a CRLF file keeps CRLF on every line",
			original: "a,b\r\n1,2,extra\r\n3,4\r\n",
			content:  "a,b\n1,2\n3,4\n",
			want:     "a,b\r\n1,2\r\n3,4\r\n",
		},
		{
			name:     "a CRLF answer to an LF file is written with LF",
			original: "select 1\nfrom t\n",
			content:  "select 2\r\nfrom t\r\n",
			want:     "select 2\nfrom t\n",
		},
		{
			name:     "a file that ends in a newline still does",
			original: "select * from a\n",
			content:  "select * from b",
			want:     "select * from b\n",
		},
		{
			name:     "a file that ends with no newline still ends with none",
			original: "{{ config(materialized='table') }}\nselect * from a",
			content:  "{{ config(materialized='table') }}\nselect * from b\n",
			want:     "{{ config(materialized='table') }}\nselect * from b",
		},
		{
			name:     "a CRLF file ending in a newline gets a CRLF one",
			original: "x\r\ny\r\n",
			content:  "x\nz",
			want:     "x\r\nz\r\n",
		},
		{
			name:     "a file with mixed line endings keeps the answer's",
			original: "a\r\nb\nc\r\n",
			content:  "a\nb\nd\n",
			want:     "a\nb\nd\n",
		},
		{
			name:     "a new file is written as answered",
			original: "",
			content:  "select 1\n",
			want:     "select 1\n",
		},
		{
			name:     "an empty answer stays empty for the checks to refuse",
			original: "select 1\r\n",
			content:  "",
			want:     "",
		},
		{
			name:     "a one-line file with no newline is left as answered",
			original: "select 1",
			content:  "select 2",
			want:     "select 2",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, KeepFileConventions(tc.original, tc.content))
		})
	}
}

func TestDraft_KeepingFileConventions(t *testing.T) {
	d := Draft{Edits: []Edit{
		{Path: "seeds/costs.csv", Original: "a\r\nb\r\n", Content: "a\nc\n"},
		{Path: "models/new.sql", Content: "select 1"},
	}, Rationale: "r"}

	got := d.KeepingFileConventions()

	assert.Equal(t, "a\r\nc\r\n", got.Edits[0].Content)
	assert.Equal(t, "select 1", got.Edits[1].Content, "an edit with no original is left as it is")
	assert.Equal(t, "a\nc\n", d.Edits[0].Content, "the receiver's edits are not changed")
	assert.Equal(t, "r", got.Rationale)
}
