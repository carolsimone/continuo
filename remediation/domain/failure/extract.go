package failure

import "regexp"

// dbtFilePathRe matches file paths in dbt log output that reference source
// files under the standard dbt directories. The match stops at whitespace or
// closing punctuation so that parenthesised context in the log does not bleed
// into the returned path.
var dbtFilePathRe = regexp.MustCompile(`(?:models|analyses|seeds)/[^\s)'"]+\.(?:sql|ya?ml|csv)`)

// extractDbtFilePath scans a dbt log string and returns the first file path
// that matches the standard dbt source layout
// (models/…, analyses/…, or seeds/… with a .sql/.yml/.yaml/.csv extension).
// Returns an empty string when no path is found.
func extractDbtFilePath(logText string) string {
	return dbtFilePathRe.FindString(logText)
}
