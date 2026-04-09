# syntax=docker/dockerfile:1.7

FROM python:3.12-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN addgroup --system appgroup && adduser --system --ingroup appgroup appuser

WORKDIR /app

RUN mkdir -p /data && chown -R appuser:appgroup /data

COPY requirements.txt ./
RUN python -m pip install --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

COPY connector ./connector

EXPOSE 8600

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD python -c "import os,urllib.request; p=os.environ.get('SERVICE_PORT','8600'); urllib.request.urlopen('http://127.0.0.1:%s/health' % p, timeout=3)"

USER appuser

CMD ["python", "-m", "connector", "serve"]
