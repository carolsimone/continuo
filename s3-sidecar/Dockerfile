FROM python:3.14-slim
# Minimal S3 I/O sidecar for compile and per-node dbt Jobs. NO dbt — keeps S3 access out
# of every dbt image. boto3 only. compile_uploader.py uploads the compiled manifest (and,
# optionally, partial-parse msgpack artifacts) to S3 (compile leg). parse_cache_fetcher.py
# downloads a partial-parse msgpack cache into a per-node Job's dbt target dir (node leg).
RUN pip install --no-cache-dir "boto3>=1.34.0"
COPY s3_common.py /s3_common.py
COPY compile_uploader.py /compile_uploader.py
COPY parse_cache_fetcher.py /parse_cache_fetcher.py

RUN useradd --uid 65532 --no-create-home --shell /usr/sbin/nologin nonroot

# Reads code from the image, writes only to stdout — run unprivileged.
USER 65532:65532

ENTRYPOINT []
CMD ["python", "/compile_uploader.py"]
