-- topology-controller reads no contract.yaml, so a service still released as
-- kind python would fail every release. This migration refuses to apply while
-- service_prod holds a service of that kind, and names the services.
DO $$
DECLARE names text;
BEGIN
  SELECT string_agg(service_name, ', ' ORDER BY service_name) INTO names
  FROM service_prod WHERE manifest_kind = 'python';
  IF names IS NOT NULL THEN
    RAISE EXCEPTION 'these services are still released as kind "python": %. Release each one as "python-odcs" (docs/contracts.md) on the continuo version you run now, then upgrade.', names;
  END IF;
END $$;
