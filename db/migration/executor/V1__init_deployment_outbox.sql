-- Deployment Outbox Table
-- Transactional outbox for deployment events

CREATE TABLE deployment_outbox (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    task_id uuid NOT NULL,
    schedule_id uuid NOT NULL,
    schedule_name character varying(255) NOT NULL,
    service_name character varying(255) NOT NULL,
    schema_name character varying(255) NOT NULL,
    table_name character varying(255) NOT NULL,
    job_name character varying(63) NOT NULL,
    status character varying(20) DEFAULT 'pending'::character varying NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    processed_at timestamp with time zone,
    retry_count integer DEFAULT 0 NOT NULL,
    max_retries integer DEFAULT 3 NOT NULL,
    error_message text,
    CONSTRAINT deployment_outbox_pkey PRIMARY KEY (id),
    CONSTRAINT deployment_outbox_status_check CHECK (((status)::text = ANY ((ARRAY['pending'::character varying, 'processed'::character varying, 'failed'::character varying])::text[])))
);

CREATE INDEX idx_deployment_outbox_status ON deployment_outbox USING btree (status, created_at) WHERE ((status)::text = 'pending'::text);
CREATE INDEX idx_deployment_outbox_task_id ON deployment_outbox USING btree (task_id);

COMMENT ON TABLE deployment_outbox IS 'Transactional outbox for deployment events';
COMMENT ON COLUMN deployment_outbox.status IS 'Processing status: pending, processed, failed';
