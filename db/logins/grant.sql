-- Runs inside a continuo database after Flyway, as the migration login, which
-- owns the database and every table in it. Gives :"login" read and write on
-- every table and sequence, now and for every table a later migration adds,
-- and nothing else: no login but the owner may create objects.
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
GRANT USAGE ON SCHEMA public TO :"login";
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO :"login";
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO :"login";
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO :"login";
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT USAGE, SELECT ON SEQUENCES TO :"login";
