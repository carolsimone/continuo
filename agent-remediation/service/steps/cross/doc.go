// Package cross holds the checks that read the drafts of every cluster of a
// try together, and the grouping of those drafts into the per-service
// verification runs the gate submits. A verification run is submitted for one
// service and checks one manifest kind, so edits that no service owns, two
// edits to one file, or a python contract mixed with dbt edits in one service
// cannot be verified, and the checks here refuse them before the gate runs.
package cross
