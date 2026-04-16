#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    CREATE DATABASE continuo_state;
    CREATE DATABASE continuo_startup;
    CREATE DATABASE continuo_executor;
    CREATE DATABASE continuo_dependency;
    CREATE DATABASE continuo_orchestrator;
    CREATE DATABASE continuo_k8s;
    CREATE DATABASE continuo_dbt;

    GRANT ALL PRIVILEGES ON DATABASE continuo_state TO $POSTGRES_USER;
    GRANT ALL PRIVILEGES ON DATABASE continuo_startup TO $POSTGRES_USER;
    GRANT ALL PRIVILEGES ON DATABASE continuo_executor TO $POSTGRES_USER;
    GRANT ALL PRIVILEGES ON DATABASE continuo_dependency TO $POSTGRES_USER;
    GRANT ALL PRIVILEGES ON DATABASE continuo_orchestrator TO $POSTGRES_USER;
    GRANT ALL PRIVILEGES ON DATABASE continuo_k8s TO $POSTGRES_USER;
    GRANT ALL PRIVILEGES ON DATABASE continuo_dbt TO $POSTGRES_USER;
EOSQL

echo "All databases created successfully"
