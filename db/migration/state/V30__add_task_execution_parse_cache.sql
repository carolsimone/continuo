-- parse_cache reports whether the dbt Job ran with the hydrated partial-parse
-- cache ('hydrated' | 'degraded' | 'unknown'); parse_cache_reason carries the
-- degrade reason. NULL for executions that predate hydration or have no
-- hydrate initContainer.
ALTER TABLE task_execution ADD COLUMN parse_cache VARCHAR(16);
ALTER TABLE task_execution ADD COLUMN parse_cache_reason TEXT;
