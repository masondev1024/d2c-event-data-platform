"""D2C application approval API with a transactionally persisted outbox."""

from __future__ import annotations

import logging
import os
import re
from time import perf_counter
from typing import Any

import psycopg
from flask import Flask, Response, g, jsonify, request
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest
from psycopg.types.json import Jsonb

from contracts import D2C_EVENT_TYPE, build_application_approved_event


LOGGER = logging.getLogger("d2c-approval-api")
IDEMPOTENCY_KEY_PATTERN = re.compile(r"^[A-Za-z0-9._:-]{8,128}$")
PRODUCTION_ENVIRONMENTS = frozenset({"production", "prod"})


def _is_production() -> bool:
    return os.environ.get("D2C_ENV", "development").lower() in PRODUCTION_ENVIRONMENTS


def _configured_trusted_hosts() -> list[str] | None:
    hosts = [item.strip() for item in os.environ.get("TRUSTED_HOSTS", "").split(",") if item.strip()]
    if _is_production() and not hosts:
        raise RuntimeError("TRUSTED_HOSTS must be configured in production")
    return hosts or None


app = Flask(__name__)
app.config.from_mapping(
    MAX_CONTENT_LENGTH=16 * 1024,
    TRUSTED_HOSTS=_configured_trusted_hosts(),
)

HTTP_REQUESTS = Counter(
    "d2c_http_requests_total",
    "HTTP requests handled by the D2C approval API.",
    ("method", "route", "status"),
)
HTTP_REQUEST_DURATION = Histogram(
    "d2c_http_request_duration_seconds",
    "D2C HTTP request duration in seconds.",
    ("method", "route"),
)
APPLY_REQUESTS = Counter(
    "d2c_apply_requests_total",
    "D2C application approval request outcomes.",
    ("result",),
)
OUTBOX_EVENTS = Counter(
    "d2c_outbox_events_total",
    "D2C transactional outbox event outcomes.",
    ("result",),
)
DB_READINESS = Gauge(
    "d2c_db_readiness",
    "Whether the D2C API can reach PostgreSQL.",
)
OUTBOX_PARITY_GAP = Gauge(
    "d2c_outbox_parity_gap",
    "Approved applications without a matching outbox event plus orphan outbox rows.",
)
OUTBOX_PARITY_CHECK = Gauge(
    "d2c_outbox_parity_check_success",
    "Whether the latest D2C approval/outbox parity query succeeded.",
)


class OutboxTransactionFailure(Exception):
    """Raised only by the validation-only failure drill."""


def get_db_connection():
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL must be configured")
    return psycopg.connect(database_url, connect_timeout=5, row_factory=psycopg.rows.dict_row)


def _metric_route() -> str:
    return request.url_rule.rule if request.url_rule is not None else "unmatched"


def _rollback_quietly(connection: Any) -> None:
    if connection is None:
        return
    try:
        connection.rollback()
    except psycopg.Error:
        LOGGER.exception("database_rollback_failed")


def _request_body() -> dict[str, Any] | None:
    body = request.get_json(silent=True)
    return body if isinstance(body, dict) else None


def _validated_apply_request(body: dict[str, Any] | None) -> tuple[int | None, int | None, str | None, str | None]:
    if body is None:
        return None, None, None, "JSON object body is required"
    user_id = body.get("user_id")
    campaign_id = body.get("campaign_id")
    idempotency_key = request.headers.get("Idempotency-Key") or body.get("idempotency_key")
    if isinstance(user_id, bool) or not isinstance(user_id, int) or user_id <= 0:
        return None, None, None, "user_id must be a positive integer"
    if isinstance(campaign_id, bool) or not isinstance(campaign_id, int) or campaign_id <= 0:
        return None, None, None, "campaign_id must be a positive integer"
    if not isinstance(idempotency_key, str) or not IDEMPOTENCY_KEY_PATTERN.fullmatch(idempotency_key):
        return None, None, None, "Idempotency-Key must be 8-128 safe characters"
    return user_id, campaign_id, idempotency_key, None


def _failure_drill_enabled() -> bool:
    return (
        os.environ.get("D2C_ENV") == "validation"
        and os.environ.get("ALLOW_FAILURE_DRILL", "").lower() == "true"
        and os.environ.get("D2C_OUTBOX_FAILURE_INJECTION") == "before_outbox_insert"
    )


def refresh_outbox_parity_metric() -> None:
    """Measure database truth, not process counters, for the release gate."""

    connection = None
    try:
        connection = get_db_connection()
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    (
                        SELECT COUNT(*)
                        FROM d2c_applications AS application
                        WHERE application.status = 'APPROVED'
                          AND NOT EXISTS (
                              SELECT 1
                              FROM d2c_outbox_events AS outbox
                              WHERE outbox.aggregate_id = application.id
                                AND outbox.event_type = %s
                          )
                    ) AS missing_events,
                    (
                        SELECT COUNT(*)
                        FROM d2c_outbox_events AS outbox
                        WHERE outbox.event_type = %s
                          AND NOT EXISTS (
                              SELECT 1
                              FROM d2c_applications AS application
                              WHERE application.id = outbox.aggregate_id
                                AND application.status = 'APPROVED'
                          )
                    ) AS orphan_events
                """,
                (D2C_EVENT_TYPE, D2C_EVENT_TYPE),
            )
            result = cursor.fetchone()
        missing_events = int(result["missing_events"])
        orphan_events = int(result["orphan_events"])
        OUTBOX_PARITY_GAP.set(missing_events + orphan_events)
        OUTBOX_PARITY_CHECK.set(1)
        DB_READINESS.set(1)
    except (psycopg.Error, OSError, RuntimeError) as error:
        # A failed parity query must fail closed for Argo: zero is reserved for
        # a successful query that found no mismatch.
        OUTBOX_PARITY_GAP.set(1)
        OUTBOX_PARITY_CHECK.set(0)
        DB_READINESS.set(0)
        LOGGER.warning("outbox_parity_check_failed error=%s", error)
    finally:
        if connection is not None:
            connection.close()


@app.before_request
def start_request_timer() -> None:
    g.request_started_at = perf_counter()


@app.after_request
def record_request_metrics(response: Response) -> Response:
    if request.endpoint != "metrics":
        HTTP_REQUESTS.labels(request.method, _metric_route(), str(response.status_code)).inc()
        HTTP_REQUEST_DURATION.labels(request.method, _metric_route()).observe(
            perf_counter() - g.get("request_started_at", perf_counter())
        )
    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    response.headers.setdefault("X-Frame-Options", "DENY")
    response.headers.setdefault("Referrer-Policy", "no-referrer")
    response.headers.setdefault("Permissions-Policy", "geolocation=(), microphone=(), camera=()")
    response.headers.setdefault(
        "Content-Security-Policy",
        "default-src 'none'; frame-ancestors 'none'; form-action 'none'",
    )
    return response


@app.get("/healthz")
def healthz() -> Response:
    return jsonify({"status": "ok"})


@app.get("/readyz")
def readyz() -> Response:
    connection = None
    try:
        connection = get_db_connection()
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1")
        DB_READINESS.set(1)
        return jsonify({"status": "ready"})
    except (psycopg.Error, OSError, RuntimeError) as error:
        DB_READINESS.set(0)
        LOGGER.warning("database_readiness_failed error=%s", error)
        return jsonify({"status": "not_ready"}), 503
    finally:
        if connection is not None:
            connection.close()


@app.get("/metrics")
def metrics() -> Response:
    refresh_outbox_parity_metric()
    return Response(generate_latest(), content_type=CONTENT_TYPE_LATEST)


@app.post("/api/apply")
def apply_application() -> Response:
    user_id, campaign_id, idempotency_key, validation_error = _validated_apply_request(_request_body())
    if validation_error:
        APPLY_REQUESTS.labels("invalid_request").inc()
        return jsonify({"status": "error", "message": validation_error}), 400

    connection = None
    try:
        connection = get_db_connection()
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO d2c_applications (user_id, campaign_id, idempotency_key)
                VALUES (%s, %s, %s)
                ON CONFLICT DO NOTHING
                RETURNING id, status, user_id, campaign_id, idempotency_key
                """,
                (user_id, campaign_id, idempotency_key),
            )
            application = cursor.fetchone()
            if application is None:
                cursor.execute(
                    """
                    SELECT id, status, user_id, campaign_id, idempotency_key
                    FROM d2c_applications
                    WHERE idempotency_key = %s
                    """,
                    (idempotency_key,),
                )
                existing_by_key = cursor.fetchone()
                if existing_by_key is not None:
                    connection.commit()
                    if (
                        existing_by_key["user_id"] != user_id
                        or existing_by_key["campaign_id"] != campaign_id
                    ):
                        APPLY_REQUESTS.labels("idempotency_conflict").inc()
                        return (
                            jsonify(
                                {
                                    "status": "error",
                                    "message": "Idempotency-Key was already used for another application",
                                }
                            ),
                            409,
                        )
                    APPLY_REQUESTS.labels("duplicate").inc()
                    return jsonify({"status": "duplicate", "application_id": existing_by_key["id"]}), 200

                cursor.execute(
                    """
                    SELECT id, status, user_id, campaign_id, idempotency_key
                    FROM d2c_applications
                    WHERE user_id = %s AND campaign_id = %s
                    """,
                    (user_id, campaign_id),
                )
                existing_by_aggregate = cursor.fetchone()
                if existing_by_aggregate is None:
                    raise psycopg.OperationalError("application conflict could not be reconciled")
                connection.commit()
                APPLY_REQUESTS.labels("duplicate").inc()
                return jsonify({"status": "duplicate", "application_id": existing_by_aggregate["id"]}), 200

            event = build_application_approved_event(
                application_id=application["id"],
                user_id=user_id,
                campaign_id=campaign_id,
            )
            if _failure_drill_enabled():
                raise OutboxTransactionFailure("validation-only outbox failure drill")
            cursor.execute(
                """
                INSERT INTO d2c_outbox_events (
                    event_id, aggregate_type, aggregate_id, event_type, event_version, payload
                ) VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    event["event_id"],
                    "d2c_application",
                    application["id"],
                    event["event_type"],
                    event["event_version"],
                    Jsonb(event),
                ),
            )
        # The application row and its event become visible together. A process
        # crash before this commit leaves neither durable record.
        connection.commit()
    except OutboxTransactionFailure as error:
        _rollback_quietly(connection)
        APPLY_REQUESTS.labels("integrity_protection_rejected").inc()
        OUTBOX_EVENTS.labels("transaction_rolled_back").inc()
        LOGGER.warning("outbox_transaction_rolled_back reason=%s", error)
        return jsonify({"status": "error", "message": "approval was not persisted"}), 503
    except (psycopg.Error, OSError, RuntimeError) as error:
        _rollback_quietly(connection)
        APPLY_REQUESTS.labels("database_error").inc()
        LOGGER.exception("application_approval_failed error=%s", error)
        return jsonify({"status": "error", "message": "approval could not be completed"}), 503
    finally:
        if connection is not None:
            connection.close()

    APPLY_REQUESTS.labels("success").inc()
    OUTBOX_EVENTS.labels("persisted").inc()
    return jsonify(
        {
            "status": "approved",
            "application_id": application["id"],
            "event_type": D2C_EVENT_TYPE,
        }
    ), 201


if __name__ == "__main__":
    if _is_production():
        raise RuntimeError("Use gunicorn to run the D2C API in production")
    app.run(
        host=os.environ.get("BIND_HOST", "127.0.0.1"),
        port=int(os.environ.get("PORT", "8080")),
    )
