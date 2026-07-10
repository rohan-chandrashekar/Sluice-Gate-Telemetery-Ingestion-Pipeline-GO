CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS telemetry_events (
    device_id       TEXT NOT NULL,
    event_time      TIMESTAMPTZ NOT NULL,
    metric          TEXT NOT NULL,
    value           DOUBLE PRECISION NOT NULL,
    idempotency_key TEXT NOT NULL
);

SELECT create_hypertable('telemetry_events', 'event_time', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_telemetry_events_device_time
    ON telemetry_events (device_id, event_time DESC);

CREATE UNIQUE INDEX IF NOT EXISTS idx_telemetry_events_idempotency
    ON telemetry_events (idempotency_key, event_time);
