FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN groupadd --system --gid 10001 app \
    && useradd --system --uid 10001 --gid 10001 --home-dir /nonexistent \
        --shell /usr/sbin/nologin app

COPY app/requirements.txt ./requirements.txt
RUN python -m pip install --requirement requirements.txt

COPY --chown=10001:10001 d2c_contract.py app/app.py app/contracts.py app/schema.py app/outbox_publisher.py ./

USER 10001:10001
EXPOSE 8080 9102

CMD ["gunicorn", "--bind", "0.0.0.0:8080", "--workers", "1", "--threads", "4", "--worker-tmp-dir", "/dev/shm", "--timeout", "30", "--access-logfile", "-", "--error-logfile", "-", "app:app"]
