-- migrate:up

-- OWNER: SightingRepository
CREATE TABLE aircraft (
    hex         TEXT        PRIMARY KEY,
    first_seen  TIMESTAMPTZ NOT NULL,
    last_seen   TIMESTAMPTZ NOT NULL,
    callsigns   TEXT[]      NOT NULL DEFAULT '{}'
);

CREATE TABLE sightings (
    id           BIGINT      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    hex          TEXT        NOT NULL REFERENCES aircraft(hex),
    callsign     TEXT,
    started_at   TIMESTAMPTZ NOT NULL,
    ended_at     TIMESTAMPTZ,
    last_seen    TIMESTAMPTZ NOT NULL,
    min_altitude INT,
    max_altitude INT,
    min_distance FLOAT,
    max_distance FLOAT
);
-- sightings is the permanent historical record of what flew overhead.
-- No retention policy — rows are small (one per session, not per position fix)
-- and the whole point is long-term queryability. ~70k rows/year at typical rates.

CREATE TABLE position_updates (
    time     TIMESTAMPTZ NOT NULL,
    hex      TEXT        NOT NULL,
    lat      FLOAT,
    lon      FLOAT,
    alt_baro INT,
    gs       FLOAT,
    track    FLOAT,
    squawk   TEXT,
    rssi     FLOAT
);
SELECT create_hypertable('position_updates', 'time');
ALTER TABLE position_updates SET (timescaledb.compress, timescaledb.compress_orderby = 'time DESC');
SELECT add_compression_policy('position_updates', INTERVAL '7 days');
SELECT add_retention_policy('position_updates', INTERVAL '90 days');

-- OWNER: EnrichmentRepository
CREATE TABLE enriched_aircraft (
    hex           TEXT        PRIMARY KEY REFERENCES aircraft(hex),
    registration  TEXT,
    type          TEXT,
    operator      TEXT,
    flag          TEXT,
    story_score   INT,
    story_tags    TEXT[]      NOT NULL DEFAULT '{}',
    annotation    TEXT,
    enriched_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at    TIMESTAMPTZ NOT NULL
);

CREATE TABLE callsign_routes (
    callsign        TEXT        PRIMARY KEY,
    origin_iata     TEXT,
    origin_icao     TEXT,
    origin_city     TEXT,
    origin_country  TEXT,
    dest_iata       TEXT,
    dest_icao       TEXT,
    dest_city       TEXT,
    dest_country    TEXT,
    fetched_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- OWNER: DigestRepository
CREATE TABLE digests (
    id              BIGINT      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    reference_date  DATE        NOT NULL,   -- period_end.date() UTC; cache key component
    n_days          INT         NOT NULL,   -- (period_end - period_start).days; cache key component
    content         TEXT        NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (reference_date, n_days)         -- enforces one digest per window per day
);

-- OWNER: UserRepository
CREATE TABLE users (
    chat_id       BIGINT      PRIMARY KEY,
    username      TEXT,
    registered_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    active        BOOLEAN     NOT NULL DEFAULT true
);

-- migrate:down

DROP TABLE IF EXISTS users;
DROP TABLE IF EXISTS digests;
DROP TABLE IF EXISTS callsign_routes;
DROP TABLE IF EXISTS enriched_aircraft;
DROP TABLE IF EXISTS position_updates;
DROP TABLE IF EXISTS sightings;
DROP TABLE IF EXISTS aircraft;
