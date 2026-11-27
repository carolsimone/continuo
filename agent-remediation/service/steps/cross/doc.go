// Package cross holds the checks that read the drafts of every cluster of a
// try together, and the grouping of those drafts into the per-project
// release checks the gate submits. A release check is submitted for one
// project and checks one manifest kind, so edits that no project owns, two
// edits to one file, or a python contract mixed with dbt edits in one project
// cannot be verified, and the checks here refuse them before the gate runs.
package cross
