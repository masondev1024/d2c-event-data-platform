FROM cgr.dev/chainguard/wolfi-base:latest@sha256:771146c2dfc880902a185f060c3db5571473c46b267edd98c3119c83068dfb61 AS runtime-base

USER root
# Install real, signed distribution packages. Keep the complete APK database,
# Python standard library and runtime libraries; build tools stay in the next
# stage. Both native-extension wheels and Python stay on the CPython 3.12 ABI.
RUN apk add --no-cache 'python-3.12>=3.12.16-r3' 'libstdc++>=16.2.0-r2' \
    && addgroup -S -g 10001 app \
    && adduser -S -D -H -u 10001 -G app -s /sbin/nologin app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    VIRTUAL_ENV=/opt/venv \
    PATH="/opt/venv/bin:$PATH"

WORKDIR /app

FROM runtime-base AS dependencies
COPY requirements.txt ./requirements.txt
RUN apk add --no-cache 'py3.12-pip>=26.2.2-r2' \
    && /usr/bin/python3.12 -m venv --without-pip /opt/venv \
    && /usr/bin/python3.12 -m pip --python /opt/venv install --no-cache-dir --requirement requirements.txt

FROM runtime-base AS base
COPY --from=dependencies /opt/venv /opt/venv

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

USER 10001:10001

HEALTHCHECK --interval=10s --timeout=5s --start-period=5s --retries=5 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8082/healthz')"

CMD ["python", "-m", "services.catalog_portal"]

FROM base AS worker

COPY d2c_contract.py ./d2c_contract.py
COPY services ./services

USER 10001:10001

CMD ["python", "-m", "services.processor"]
