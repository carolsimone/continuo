-- Bundled quickstart only. Runs inside the warehouse database as the bundled
-- superuser and hands every schema, table, view, materialized view, foreign
-- table, sequence and routine that :"login" does not own to it, one object at
-- a time, so dbt, signed in as :"login", can replace them. Objects that belong
-- to an extension, and the system schemas, are left alone. REASSIGN OWNED is
-- not used: it would also hand over every database the superuser owns.
\set VERBOSITY terse
SELECT format('ALTER SCHEMA %I OWNER TO %I', n.nspname, :'login')
FROM pg_namespace n
WHERE n.nspname NOT LIKE 'pg\_%' AND n.nspname <> 'information_schema'
  AND n.nspowner NOT IN (:'login'::regrole, 'pg_database_owner'::regrole)
  AND NOT EXISTS (SELECT 1 FROM pg_depend d
                  WHERE d.classid = 'pg_namespace'::regclass AND d.objid = n.oid AND d.deptype = 'e')
\gexec
-- Tables first: a table's identity and serial sequences follow it.
SELECT format('ALTER %s %s OWNER TO %I',
              CASE c.relkind WHEN 'v' THEN 'VIEW' WHEN 'm' THEN 'MATERIALIZED VIEW'
                             WHEN 'f' THEN 'FOREIGN TABLE' ELSE 'TABLE' END,
              c.oid::regclass, :'login')
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('r', 'p', 'v', 'm', 'f')
  AND n.nspname NOT LIKE 'pg\_%' AND n.nspname <> 'information_schema'
  AND c.relowner <> :'login'::regrole
  AND NOT EXISTS (SELECT 1 FROM pg_depend d
                  WHERE d.classid = 'pg_class'::regclass AND d.objid = c.oid AND d.deptype = 'e')
ORDER BY c.oid
\gexec
SELECT format('ALTER SEQUENCE %s OWNER TO %I', c.oid::regclass, :'login')
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind = 'S'
  AND n.nspname NOT LIKE 'pg\_%' AND n.nspname <> 'information_schema'
  AND c.relowner <> :'login'::regrole
  AND NOT EXISTS (SELECT 1 FROM pg_depend d
                  WHERE d.classid = 'pg_class'::regclass AND d.objid = c.oid AND d.deptype IN ('e', 'a', 'i'))
\gexec
SELECT format('ALTER ROUTINE %s OWNER TO %I', p.oid::regprocedure, :'login')
FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
WHERE n.nspname NOT LIKE 'pg\_%' AND n.nspname <> 'information_schema'
  AND p.proowner <> :'login'::regrole
  AND NOT EXISTS (SELECT 1 FROM pg_depend d
                  WHERE d.classid = 'pg_proc'::regclass AND d.objid = p.oid AND d.deptype = 'e')
\gexec
