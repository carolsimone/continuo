FROM python:3.12-slim

RUN pip install --no-cache-dir dbt-postgres

WORKDIR /project

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
