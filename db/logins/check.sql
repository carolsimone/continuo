-- Runs inside :"own_db", the database of the runtime login :'login', as the
-- migration login :'migration_login', after the grants. Prints one line per
-- right the login holds beyond its own, or lacks; migrate-all.sh stops the
-- deploy when it prints any. :'dbs' lists every database (comma-separated)
-- the login must not connect to unless it is :'own_db'.
WITH r AS (
  SELECT * FROM pg_roles WHERE rolname = :'login'
), problems(problem) AS (
  SELECT 'cannot CONNECT to its own database ' || :'own_db'
  WHERE NOT has_database_privilege(:'login', :'own_db', 'CONNECT')
  UNION ALL
  SELECT 'can CONNECT to ' || db
  FROM unnest(string_to_array(:'dbs', ',')) AS db
  WHERE db <> :'own_db'
    AND EXISTS (SELECT 1 FROM pg_database WHERE datname = db)
    AND has_database_privilege(:'login', db, 'CONNECT')
  UNION ALL
  SELECT 'has CREATE on schema public in ' || :'own_db'
  WHERE has_schema_privilege(:'login', 'public', 'CREATE')
  UNION ALL
  SELECT format('lacks %s on table %s', priv, c.oid::regclass)
  FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
  CROSS JOIN unnest(ARRAY['SELECT', 'INSERT', 'UPDATE', 'DELETE']) AS priv
  WHERE n.nspname = 'public' AND c.relkind IN ('r', 'p')
    AND NOT has_table_privilege(:'login', c.oid, priv)
  UNION ALL SELECT 'is a superuser' FROM r WHERE rolsuper
  UNION ALL SELECT 'has CREATEROLE' FROM r WHERE rolcreaterole
  UNION ALL SELECT 'has CREATEDB' FROM r WHERE rolcreatedb
  UNION ALL SELECT 'has REPLICATION' FROM r WHERE rolreplication
  UNION ALL SELECT 'has BYPASSRLS' FROM r WHERE rolbypassrls
  UNION ALL
  SELECT 'is a member of ' || :'migration_login'
  WHERE :'login' <> :'migration_login'
    AND pg_has_role(:'login', :'migration_login', 'MEMBER')
  UNION ALL
  SELECT 'owns database ' || datname FROM pg_database WHERE datdba = :'login'::regrole
  UNION ALL
  SELECT format('owns %s in %s', c.oid::regclass, :'own_db')
  FROM pg_class c WHERE c.relowner = :'login'::regrole
  UNION ALL
  SELECT format('owns schema %I in %s', nspname, :'own_db')
  FROM pg_namespace WHERE nspowner = :'login'::regrole
  UNION ALL
  SELECT format('owns %s in %s', p.oid::regprocedure, :'own_db')
  FROM pg_proc p WHERE p.proowner = :'login'::regrole
)
SELECT format('login %s: %s', :'login', problem) FROM problems ORDER BY 1;
