-- Startup Outbox Table
-- Transactional outbox for startup orchestration events

CREATE TABLE startup_outbox (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    aggregate_type character varying(50) NOT NULL,
    aggregate_id uuid NOT NULL,
    event_type character varying(50) NOT NULL,
    payload jsonb NOT NULL,
    stream_name character varying(100) NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    processed_at timestamp with time zone,
    status character varying(20) DEFAULT 'pending'::character varying NOT NULL,
    retry_count integer DEFAULT 0 NOT NULL,
    max_retries integer DEFAULT 3 NOT NULL,
    error_message text,
    CONSTRAINT startup_outbox_pkey PRIMARY KEY (id),
    CONSTRAINT startup_outbox_status_check CHECK (((status)::text = ANY ((ARRAY['pending'::character varying, 'processed'::character varying, 'failed'::character varying])::text[])))
);

CREATE INDEX idx_startup_outbox_aggregate ON startup_outbox USING btree (aggregate_type, aggregate_id);
CREATE INDEX idx_startup_outbox_status_created ON startup_outbox USING btree (status, created_at) WHERE ((status)::text = 'pending'::text);

COMMENT ON TABLE startup_outbox IS 'Transactional outbox for startup orchestration events';
COMMENT ON COLUMN startup_outbox.aggregate_type IS 'Type of aggregate (e.g., scheduler)';
COMMENT ON COLUMN startup_outbox.event_type IS 'Type of event (e.g., node_ready_for_execution)';
COMMENT ON COLUMN startup_outbox.stream_name IS 'Target Redis stream name';
