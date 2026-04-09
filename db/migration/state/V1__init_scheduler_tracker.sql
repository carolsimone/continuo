-- Scheduler Tracker Table
-- Tracks the lifecycle of scheduled workflows

CREATE TABLE scheduler_tracker (
    schedule_id uuid DEFAULT gen_random_uuid() NOT NULL,
    schedule_name character varying(50) NOT NULL,
    status character varying(20) NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    started_at timestamp with time zone,
    completed_at timestamp with time zone,
    last_heartbeat_at timestamp with time zone,
    cancelled_at timestamp with time zone,
    cancelled_by character varying(255),
    cancellation_reason text,
    initialization_status character varying(20) DEFAULT 'pending'::character varying NOT NULL,
    CONSTRAINT scheduler_tracker_pkey PRIMARY KEY (schedule_id),
    CONSTRAINT scheduler_tracker_initialization_status_check CHECK (((initialization_status)::text = ANY ((ARRAY['pending'::character varying, 'in_progress'::character varying, 'completed'::character varying, 'failed'::character varying])::text[]))),
    CONSTRAINT scheduler_tracker_status_check CHECK (((status)::text = ANY ((ARRAY['pending'::character varying, 'running'::character varying, 'succeeded'::character varying, 'failed'::character varying, 'cancelled'::character varying])::text[]))),
    CONSTRAINT valid_timestamps CHECK ((((started_at IS NULL) OR (started_at >= created_at)) AND ((completed_at IS NULL) OR (completed_at >= started_at))))
);

CREATE INDEX idx_scheduler_tracker_init_status ON scheduler_tracker USING btree (schedule_id, initialization_status);

COMMENT ON TABLE scheduler_tracker IS 'Tracks the lifecycle of scheduled workflows';
COMMENT ON COLUMN scheduler_tracker.schedule_id IS 'Unique identifier for the schedule';
COMMENT ON COLUMN scheduler_tracker.initialization_status IS 'Status of dependency graph initialization';
