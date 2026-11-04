-- Prints one line per login in :'logins' (comma-separated) that does not
-- exist. migrate-all.sh runs it before it changes anything and stops when it
-- prints a line.
SELECT format('login %s does not exist: create it as described in deploy/CREDENTIALS.md', l)
FROM unnest(string_to_array(:'logins', ',')) AS l
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = l)
ORDER BY l;
