FROM python:3.12-slim
# Minimal manifest-upload sidecar for the compile Job's main container. NO dbt — keeps the compile
# leg's S3 access out of any dbt image. boto3 only.
RUN pip install --no-cache-dir "boto3>=1.34.0"
COPY compile_uploader.py /compile_uploader.py
ENTRYPOINT []
CMD ["python", "/compile_uploader.py"]
