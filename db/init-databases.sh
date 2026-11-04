#!/bin/bash
# docker-compose only: runs once, on Postgres's first start, as the migration
# login continuo_svc (the image's superuser). Acts as a company would: it
# creates every continuo database, each service's login and the warehouse
# login, each with the local password local-<login>. The migrations service
# then grants each login its rights (db/migrate-all.sh).
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    CREATE DATABASE continuo_run_controller;
    CREATE DATABASE continuo_execution;
    CREATE DATABASE continuo_dbt;
    CREATE DATABASE continuo_release;
    CREATE DATABASE continuo_agent_chat;
    CREATE DATABASE continuo_remediation;
    CREATE DATABASE continuo_agent_remediation;
    CREATE DATABASE continuo_dead_letter;

    CREATE ROLE continuo_run_controller LOGIN PASSWORD 'local-continuo_run_controller';
    CREATE ROLE continuo_execution_controller LOGIN PASSWORD 'local-continuo_execution_controller';
    CREATE ROLE continuo_release_controller LOGIN PASSWORD 'local-continuo_release_controller';
    CREATE ROLE continuo_remediation LOGIN PASSWORD 'local-continuo_remediation';
    CREATE ROLE continuo_agent_remediation LOGIN PASSWORD 'local-continuo_agent_remediation';
    CREATE ROLE continuo_agent_chat LOGIN PASSWORD 'local-continuo_agent_chat';
    CREATE ROLE continuo_dead_letter_controller LOGIN PASSWORD 'local-continuo_dead_letter_controller';

    -- The dbt warehouse belongs to its own login, as a company's would; only
    -- it connects to it.
    CREATE ROLE continuo_warehouse LOGIN PASSWORD 'local-continuo_warehouse';
    ALTER DATABASE continuo_dbt OWNER TO continuo_warehouse;
    REVOKE CONNECT ON DATABASE continuo_dbt FROM PUBLIC;
EOSQL

echo "All databases and logins created successfully"
