-- Run-history percentiles from state's database (continuo_state). Read-only.
--   psql -v kinds=cron,trigger -v days=30 -f run_history.sql
-- busy_s = union of every attempt's container execution interval; idle_s = wall_s - busy_s.
SET default_transaction_read_only = on;
\if :{?kinds} \else \set kinds 'cron' \endif
\if :{?days} \else \set days 30 \endif

WITH runs AS (
  SELECT schedule_id, schedule_name, status, created_at, completed_at
  FROM scheduler_tracker
  WHERE kind = ANY (string_to_array(:'kinds', ','))
    AND completed_at IS NOT NULL
    AND created_at > now() - make_interval(days => :days)
),
ex AS (
  SELECT t.schedule_id, te.started_at, te.completed_at
  FROM task_tracker t
  JOIN task_execution te ON te.task_id = t.task_id
  JOIN runs r ON r.schedule_id = t.schedule_id
  WHERE te.started_at IS NOT NULL AND te.completed_at IS NOT NULL
),
ordered AS (
  SELECT schedule_id, started_at, completed_at,
         max(completed_at) OVER (PARTITION BY schedule_id ORDER BY started_at, completed_at
                                 ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS prev_max_end
  FROM ex
),
islands AS (
  SELECT schedule_id, started_at, completed_at,
         sum(CASE WHEN prev_max_end IS NULL OR started_at > prev_max_end THEN 1 ELSE 0 END)
           OVER (PARTITION BY schedule_id ORDER BY started_at, completed_at) AS island
  FROM ordered
),
busy AS (
  SELECT schedule_id, sum(extract(epoch FROM island_end - island_start)) AS busy_s
  FROM (SELECT schedule_id, island, min(started_at) AS island_start, max(completed_at) AS island_end
        FROM islands GROUP BY schedule_id, island) s
  GROUP BY schedule_id
),
per_run AS (
  SELECT r.schedule_name, r.status,
         extract(epoch FROM r.completed_at - r.created_at) AS wall_s,
         extract(epoch FROM min(e.started_at) - r.created_at) AS first_start_s,
         extract(epoch FROM r.completed_at - max(e.completed_at)) AS finalize_s,
         b.busy_s
  FROM runs r
  JOIN ex e ON e.schedule_id = r.schedule_id
  JOIN busy b ON b.schedule_id = r.schedule_id
  GROUP BY r.schedule_name, r.schedule_id, r.status, r.created_at, r.completed_at, b.busy_s
)
SELECT schedule_name, status, count(*) AS runs,
       round(percentile_cont(0.5)  WITHIN GROUP (ORDER BY wall_s)::numeric, 1)          AS p50_wall_s,
       round(percentile_cont(0.95) WITHIN GROUP (ORDER BY wall_s)::numeric, 1)          AS p95_wall_s,
       round(percentile_cont(0.5)  WITHIN GROUP (ORDER BY busy_s)::numeric, 1)          AS p50_busy_s,
       round(percentile_cont(0.5)  WITHIN GROUP (ORDER BY wall_s - busy_s)::numeric, 1) AS p50_idle_s,
       round(percentile_cont(0.95) WITHIN GROUP (ORDER BY wall_s - busy_s)::numeric, 1) AS p95_idle_s,
       round(percentile_cont(0.5)  WITHIN GROUP (ORDER BY first_start_s)::numeric, 1)   AS p50_first_start_s,
       round(percentile_cont(0.5)  WITHIN GROUP (ORDER BY finalize_s)::numeric, 1)      AS p50_finalize_s
FROM per_run
GROUP BY schedule_name, status
ORDER BY schedule_name, status;

SELECT schedule_name, kind, cancelled_by, left(cancellation_reason, 80) AS reason, count(*)
FROM scheduler_tracker
WHERE status = 'cancelled' AND created_at > now() - make_interval(days => :days)
GROUP BY 1, 2, 3, 4
ORDER BY 5 DESC;
