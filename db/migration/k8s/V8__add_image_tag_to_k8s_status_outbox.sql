-- Add image_tag to k8s_status_outbox so the retry event (retry.task:v1) can carry
-- the exact image tag that was deployed, which executor-controller needs to build
-- the pod spec for the retry k8s job.
-- DEFAULT '' covers existing in-flight rows (old executor-controller did not publish image_tag).
ALTER TABLE k8s_status_outbox ADD COLUMN image_tag TEXT NOT NULL DEFAULT '';
