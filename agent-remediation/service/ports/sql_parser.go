package ports

import "context"

// SQLParser parses one SQL file in the dialect of the install's warehouse.
// Parse answers what the parser found: the errors, each with its line, or the
// reason it did not parse the file at all. An error from Parse means the
// parser could not answer, not that the SQL is wrong.
type SQLParser interface {
	Parse(ctx context.Context, sql string) (SQLParseResult, error)
}

// SQLParseResult is what parsing one file found. Errors is empty when the SQL
// parses. Skipped, when set, is why the file was not parsed, and Errors is
// then empty.
type SQLParseResult struct {
	Errors  []SQLParseError
	Skipped string
}

// SQLParseError is one parse error and the line of the file it is on,
// counted from 1.
type SQLParseError struct {
	Message string
	Line    int
}
