-- ─────────────────────────────────────────────────────────────
-- Weather Streaming Pipeline — Database Initialisation
-- ─────────────────────────────────────────────────────────────

-- Create dedicated user for the streaming pipeline
CREATE USER streaming_user WITH PASSWORD 'streaming_pass';

-- Create database owned by streaming_user
CREATE DATABASE weather_streaming OWNER streaming_user;

-- Grant all privileges
GRANT ALL PRIVILEGES ON DATABASE weather_streaming TO streaming_user;

-- Connect to the new database
\c weather_streaming;

-- ─────────────────────────────────────────────────────────────
-- Main weather events table
-- Stores every validated weather record consumed from Kafka
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS weather_events (
    id                   SERIAL PRIMARY KEY,
    city                 VARCHAR(100)  NOT NULL,
    country              VARCHAR(10)   NOT NULL,
    temperature          FLOAT         NOT NULL,
    feels_like           FLOAT         NOT NULL,
    humidity             INTEGER       NOT NULL CHECK (humidity >= 0 AND humidity <= 100),
    pressure             INTEGER       NOT NULL CHECK (pressure >= 800 AND pressure <= 1100),
    weather_description  VARCHAR(255)  NOT NULL,
    wind_speed           FLOAT         NOT NULL CHECK (wind_speed >= 0),
    visibility           INTEGER       DEFAULT 0,
    recorded_at          TIMESTAMP     NOT NULL,
    inserted_at          TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,
    kafka_offset         BIGINT,       -- Tracks which Kafka message this came from
    kafka_partition      INTEGER       -- Tracks which Kafka partition this came from
);

-- ─────────────────────────────────────────────────────────────
-- Dead letter queue table
-- Stores messages that failed validation or processing
-- so they can be investigated and reprocessed later
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS weather_events_dlq (
    id            SERIAL PRIMARY KEY,
    raw_message   TEXT          NOT NULL,  -- The original Kafka message as-is
    error_type    VARCHAR(100)  NOT NULL,  -- e.g. ValidationError, DBError
    error_detail  TEXT          NOT NULL,  -- Full error message for debugging
    kafka_topic   VARCHAR(255),
    kafka_offset  BIGINT,
    failed_at     TIMESTAMP     DEFAULT CURRENT_TIMESTAMP
);

-- ─────────────────────────────────────────────────────────────
-- Indexes for fast querying
-- ─────────────────────────────────────────────────────────────
CREATE INDEX idx_events_city         ON weather_events(city);
CREATE INDEX idx_events_recorded_at  ON weather_events(recorded_at);
CREATE INDEX idx_events_country      ON weather_events(country);
CREATE INDEX idx_events_city_time    ON weather_events(city, recorded_at DESC);

-- ─────────────────────────────────────────────────────────────
-- Permissions
-- ─────────────────────────────────────────────────────────────
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO streaming_user;