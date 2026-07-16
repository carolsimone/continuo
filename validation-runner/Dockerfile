FROM python:3.12-slim

# Minimal continuo-owned image for blue/green validation. NO dbt: validation builds
# each node as an empty table in the candidate schema from its compiled SQL (psycopg2),
# fetching its own candidate SQL from S3 (boto3). dbt and the per-service team image are
# only ever run (by the executor) for compile, seed-build, and scheduled runs.
RUN pip install --no-cache-dir "psycopg2-binary>=2.9.0" "boto3>=1.34.0"

# Flat layout at /: validation_runner.py imports warehouse / validation_result /
# s3_common as top-level modules (the script's dir is on sys.path at run time).
COPY validation_runner.py /validation_runner.py
COPY warehouse.py /warehouse.py
COPY validation_result.py /validation_result.py
COPY s3_common.py /s3_common.py

RUN useradd --uid 65532 --no-create-home --shell /usr/sbin/nologin nonroot

# Reads code from the image, writes only to stdout — run unprivileged.
USER 65532:65532

ENTRYPOINT []
CMD ["python", "/validation_runner.py"]
