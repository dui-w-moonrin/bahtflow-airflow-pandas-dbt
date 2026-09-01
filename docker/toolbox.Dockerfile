FROM python:3.12-slim

WORKDIR /opt/bahtflow

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

CMD ["sleep", "infinity"]
