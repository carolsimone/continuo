-- Failure attribution for compile-leg deployments: the name of the pod
-- container that terminated non-zero (compile | parse-prod | parse-candidate
-- | upload). NULL for successful outcomes and for pre-attribution rows.
ALTER TABLE executor_deployments ADD COLUMN failed_container TEXT;
