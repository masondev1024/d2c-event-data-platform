FROM cgr.dev/chainguard/wolfi-base:latest@sha256:771146c2dfc880902a185f060c3db5571473c46b267edd98c3119c83068dfb61 AS runtime-base

USER root
RUN apk add --no-cache 'python-3.12>=3.12.16-r3' 'libstdc++>=16.2.0-r2' \
    && addgroup -S -g 10001 app \
    && adduser -S -D -H -u 10001 -G app -s /sbin/nologin app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    VIRTUAL_ENV=/opt/venv \
    PATH="/opt/venv/bin:$PATH"

WORKDIR /app

FROM runtime-base AS dependencies
COPY app/requirements.txt ./requirements.txt
RUN apk add --no-cache 'py3.12-pip>=26.2.2-r2' \
    && /usr/bin/python3.12 -m venv --without-pip /opt/venv \
    && /usr/bin/python3.12 -m pip --python /opt/venv install --no-cache-dir --requirement requirements.txt

FROM runtime-base AS runtime
COPY --from=dependencies /opt/venv /opt/venv

COPY --chown=10001:10001 d2c_contract.py app/app.py app/contracts.py app/schema.py app/outbox_publisher.py ./

USER 10001:10001
EXPOSE 8080 9102

CMD ["gunicorn", "--bind", "0.0.0.0:8080", "--workers", "1", "--threads", "4", "--worker-tmp-dir", "/dev/shm", "--timeout", "30", "--access-logfile", "-", "--error-logfile", "-", "app:app"]
