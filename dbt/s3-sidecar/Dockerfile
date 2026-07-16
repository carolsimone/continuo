FROM python:3.12-slim
# Minimal S3 I/O sidecar for the compile Job. NO dbt — keeps S3 access out of every dbt
# image. boto3 only. compile_uploader.py uploads the compiled manifest to S3 (compile leg).
RUN pip install --no-cache-dir "boto3>=1.34.0"
COPY s3_common.py /s3_common.py
COPY compile_uploader.py /compile_uploader.py

RUN useradd --uid 65532 --no-create-home --shell /usr/sbin/nologin nonroot

# Reads code from the image, writes only to stdout — run unprivileged.
USER 65532:65532

ENTRYPOINT []
CMD ["python", "/compile_uploader.py"]
