"""Repeatable PostgreSQL schema migration for the D2C approval flow."""

from __future__ import annotations

import os

import psycopg
from psycopg.rows import dict_row


SCHEMA_STATEMENTS = (
    """
    CREATE TABLE IF NOT EXISTS d2c_campaigns (
        id BIGSERIAL PRIMARY KEY,
        name VARCHAR(120) NOT NULL UNIQUE,
        active BOOLEAN NOT NULL DEFAULT TRUE,
        created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS d2c_applications (
        id BIGSERIAL PRIMARY KEY,
        user_id BIGINT NOT NULL,
        campaign_id BIGINT NOT NULL REFERENCES d2c_campaigns(id),
        status VARCHAR(16) NOT NULL DEFAULT 'APPROVED',
        idempotency_key VARCHAR(128) NOT NULL UNIQUE,
        approved_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        CONSTRAINT d2c_application_status_check CHECK (status = 'APPROVED'),
        CONSTRAINT d2c_user_campaign_unique UNIQUE (user_id, campaign_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS d2c_outbox_events (
        event_id UUID PRIMARY KEY,
        aggregate_type VARCHAR(64) NOT NULL,
        aggregate_id BIGINT NOT NULL REFERENCES d2c_applications(id),
        event_type VARCHAR(128) NOT NULL,
        event_version INTEGER NOT NULL CHECK (event_version > 0),
        payload JSONB NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        published_at TIMESTAMPTZ NULL,
        publish_attempts INTEGER NOT NULL DEFAULT 0,
        last_error VARCHAR(1024) NULL,
        CONSTRAINT d2c_outbox_aggregate_event_unique UNIQUE (event_type, aggregate_id)
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS d2c_outbox_unpublished_idx
        ON d2c_outbox_events (created_at)
        WHERE published_at IS NULL
    """,
    """
    INSERT INTO d2c_campaigns (name)
    SELECT 'Portfolio D2C approval campaign'
    WHERE NOT EXISTS (
        SELECT 1 FROM d2c_campaigns WHERE name = 'Portfolio D2C approval campaign'
    )
    """,
)


def connect(database_url: str | None = None):
    """Open a bounded-time database connection for migration or readiness checks."""

    dsn = database_url or os.environ.get("DATABASE_URL")
    if not dsn:
        raise RuntimeError("DATABASE_URL must be configured")
    return psycopg.connect(dsn, connect_timeout=5, row_factory=dict_row)


def apply_schema_migrations(connection) -> None:
    """Apply additive DDL in one transaction and commit only when all statements pass."""

    with connection.cursor() as cursor:
        for statement in SCHEMA_STATEMENTS:
            cursor.execute(statement)
    connection.commit()


def main() -> int:
    connection = connect()
    try:
        apply_schema_migrations(connection)
    finally:
        connection.close()
    print("D2C schema migrations completed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
