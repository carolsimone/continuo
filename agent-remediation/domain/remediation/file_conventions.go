package remediation

import "strings"

// KeepFileConventions returns content written the way original is written, so
// an edit changes only the lines the fix changes. A model answers with LF line
// endings and its own choice of a final newline whatever the file uses; a
// diff of that answer against a CRLF file, or against a file that ends
// differently, marks every line, or the last one, as changed.
//
//   - Line endings: when every line break of original is CRLF, content is
//     written with CRLF; when original has only LF breaks, content is written
//     with LF. A file that mixes the two keeps content's breaks as answered.
//   - The final newline: content ends in a line break exactly when original
//     does, in original's style. An original with no line break at all says
//     nothing about it, so content is left as answered.
//
// An empty original (a file the fix creates) and an empty content (an answer
// the pre-flight checks refuse) are returned unchanged.
func KeepFileConventions(original, content string) string {
	if original == "" || content == "" {
		return content
	}
	breaks := strings.Count(original, "\n")
	if breaks == 0 {
		return content
	}
	crlf := strings.Count(original, "\r\n")
	newline := ""
	switch crlf {
	case breaks:
		newline = "\r\n"
	case 0:
		newline = "\n"
	}
	out := content
	if newline != "" {
		out = strings.ReplaceAll(out, "\r\n", "\n")
		if newline == "\r\n" {
			out = strings.ReplaceAll(out, "\n", "\r\n")
		}
	}
	endsInBreak := strings.HasSuffix(original, "\n")
	switch {
	case endsInBreak && !strings.HasSuffix(out, "\n"):
		if newline == "" {
			newline = "\n"
		}
		out += newline
	case !endsInBreak && strings.HasSuffix(out, "\n"):
		out = strings.TrimSuffix(out, "\n")
		out = strings.TrimSuffix(out, "\r")
	}
	return out
}

// KeepingFileConventions returns d with the content of every edit that has an
// original written the way that original is (KeepFileConventions). An edit
// with no original is left as it is. d's own edits are not changed.
func (d Draft) KeepingFileConventions() Draft {
	edits := make([]Edit, len(d.Edits))
	copy(edits, d.Edits)
	for i, e := range edits {
		edits[i].Content = KeepFileConventions(e.Original, e.Content)
	}
	d.Edits = edits
	return d
}
