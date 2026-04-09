#!/bin/sh
set -e

: "${TABLE_NAME:?TABLE_NAME must be set}"

echo "schedule_name=${SCHEDULE_NAME} table_name=${TABLE_NAME} schema_name=${SCHEMA} job_name=${JOB_NAME} service_name=${SERVICE_NAME}"

exec dbt run --select "${TABLE_NAME}" --profiles-dir /project
