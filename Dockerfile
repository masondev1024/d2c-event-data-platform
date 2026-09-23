FROM python:3.12-slim@sha256:2f17fc044b579bab302c2e8054d3a686e2cb9a83de48e70534b94cd8ebbe06a9 AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt ./requirements.txt
RUN pip install --no-cache-dir --requirement requirements.txt

FROM base AS catalog-portal

# The portal validates evidence references at startup. Keep the evidence set
# explicit so the worker image does not receive unrelated application files.
COPY catalog ./catalog
COPY RUNBOOK.md ./RUNBOOK.md
COPY d2c_contract.py ./d2c_contract.py
COPY docker-compose.yml ./docker-compose.yml
COPY schemas/d2c.application.approved.v1.schema.json ./schemas/d2c.application.approved.v1.schema.json
COPY lakehouse ./lakehouse
COPY app ./app
COPY services ./services

RUN groupadd --system --gid 10001 app \
    && useradd --system --uid 10001 --gid 10001 --home-dir /nonexistent \
        --shell /usr/sbin/nologin app

USER 10001:10001

HEALTHCHECK --interval=10s --timeout=5s --start-period=5s --retries=5 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8082/healthz')"

CMD ["python", "-m", "services.catalog_portal"]

FROM base AS worker

COPY d2c_contract.py ./d2c_contract.py
COPY services ./services

RUN groupadd --system --gid 10001 app \
    && useradd --system --uid 10001 --gid 10001 --home-dir /nonexistent \
        --shell /usr/sbin/nologin app

USER 10001:10001

CMD ["python", "-m", "services.processor"]
