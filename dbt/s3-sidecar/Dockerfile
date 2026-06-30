FROM python:3.12-slim
# Minimal S3 I/O sidecar for the compile + validation Jobs. NO dbt — keeps S3 access out of
# every dbt image. boto3 only. The pod spec selects the script via the container command:
# compile_uploader.py uploads the compiled manifest (compile leg); candidate_fetcher.py
# downloads a node's candidate SQL into the shared emptyDir (validation leg).
RUN pip install --no-cache-dir "boto3>=1.34.0"
COPY compile_uploader.py /compile_uploader.py
COPY candidate_fetcher.py /candidate_fetcher.py
ENTRYPOINT []
CMD ["python", "/compile_uploader.py"]
