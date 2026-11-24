// Package rename holds the steps that move a dbt model to a new name in the
// same schema, the fix for a model whose relation another node already
// builds: the model_rename context step plans the move, the duplicate-table
// prompt has the model apply it, and the rename_matches_plan check holds the
// draft to the plan.
package rename
