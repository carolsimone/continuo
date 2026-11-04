-- Bundled quickstart only: creates :"login", or resets it, as a login with a
-- password and no other attribute. The password comes from the environment
-- variable CONTINUO_PG_LOGIN_PASSWORD, so it never appears on a command line.
\set VERBOSITY terse
\getenv password CONTINUO_PG_LOGIN_PASSWORD
SELECT format('CREATE ROLE %I LOGIN', :'login')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'login')
\gexec
SELECT format('ALTER ROLE %I WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD %L',
              :'login', :'password')
\gexec
