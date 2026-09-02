FROM apache/airflow:3.3.1-python3.12

COPY requirements-gcp.txt /tmp/requirements-gcp.txt
RUN pip install --no-cache-dir -r /tmp/requirements-gcp.txt
