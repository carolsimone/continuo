-- Outbox Table
-- Transactional outbox for dependency orchestration events

CREATE TABLE outbox (
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
    CONSTRAINT outbox_pkey PRIMARY KEY (id),
    CONSTRAINT outbox_status_check CHECK (((status)::text = ANY ((ARRAY['pending'::character varying, 'processed'::character varying, 'failed'::character varying])::text[])))
);

CREATE INDEX idx_outbox_aggregate ON outbox USING btree (aggregate_type, aggregate_id);
CREATE INDEX idx_outbox_status_created ON outbox USING btree (status, created_at) WHERE ((status)::text = 'pending'::text);

COMMENT ON TABLE outbox IS 'Transactional outbox for dependency orchestration events';
COMMENT ON COLUMN outbox.aggregate_type IS 'Type of aggregate (e.g., dependency)';
COMMENT ON COLUMN outbox.event_type IS 'Type of event (e.g., node_ready_for_execution)';
COMMENT ON COLUMN outbox.stream_name IS 'Target Redis stream name';
