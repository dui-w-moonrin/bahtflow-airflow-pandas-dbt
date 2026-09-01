FROM python:3.12-slim

WORKDIR /opt/bahtflow

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

COPY requirements-gcp.txt /tmp/requirements-gcp.txt
RUN pip install --no-cache-dir -r /tmp/requirements-gcp.txt

CMD ["sleep", "infinity"]
