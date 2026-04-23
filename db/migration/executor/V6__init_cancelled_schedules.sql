CREATE TABLE cancelled_schedules (
    schedule_id  UUID        PRIMARY KEY,
    cancelled_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE cancelled_schedules IS
  'Local guard table populated from schedule.cancelled:v1 stream. '
  'Rows are swept after a configurable TTL (default 24h).';
