-- Message Processing Summary
SELECT
    'Message Processing' as metric_type,
    state,
    COUNT(*) as count,
    EXTRACT(EPOCH FROM (NOW() - MIN(created_at))) as oldest_seconds
FROM message_processing
GROUP BY state

UNION ALL

-- Outbox Summary
SELECT
    'Outbox' as metric_type,
    status as state,
    COUNT(*) as count,
    EXTRACT(EPOCH FROM (NOW() - MIN(created_at))) as oldest_seconds
FROM outbox
GROUP BY status

ORDER BY metric_type, state;
