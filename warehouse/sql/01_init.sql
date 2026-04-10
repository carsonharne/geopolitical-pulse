-- File: warehouse/sql/01_init.sql
-- Description: PostgreSQL initialization script for The Geopolitical Supply Chain Pulse
-- Creates the raw events table for GDELT data ingestion from Kafka

-- Drop table if exists for idempotent execution
DROP TABLE IF EXISTS raw_gdelt_events;

-- Create raw events table
-- Stores incoming GDELT events from the Kafka topic gdelt-china-events
CREATE TABLE raw_gdelt_events (
    globaleventid BIGINT PRIMARY KEY,
    sqldate VARCHAR(8) NOT NULL,
    eventcode VARCHAR(4),
    goldsteinscale FLOAT,
    nummentions INT,
    actiongeo_countrycode VARCHAR(3),
    sourceurl TEXT,
    ingested_at TIMESTAMP DEFAULT NOW()
);

-- Create index on sqldate for efficient date-based queries
CREATE INDEX idx_raw_gdelt_events_sqldate ON raw_gdelt_events(sqldate);

-- Create index on eventcode for filtering by event type
CREATE INDEX idx_raw_gdelt_events_eventcode ON raw_gdelt_events(eventcode);

-- Create index on ingested_at for time-based queries and cleanup
CREATE INDEX idx_raw_gdelt_events_ingested_at ON raw_gdelt_events(ingested_at);

COMMENT ON TABLE raw_gdelt_events IS 'Raw GDELT events ingested from Kafka topic gdelt-china-events';
COMMENT ON COLUMN raw_gdelt_events.globaleventid IS 'Unique GDELT event identifier';
COMMENT ON COLUMN raw_gdelt_events.sqldate IS 'Event date in YYYYMMDD format';
COMMENT ON COLUMN raw_gdelt_events.eventcode IS 'CAMEO event code';
COMMENT ON COLUMN raw_gdelt_events.goldsteinscale IS 'Event impact score (-10 to +10)';
COMMENT ON COLUMN raw_gdelt_events.nummentions IS 'Number of media mentions';
COMMENT ON COLUMN raw_gdelt_events.actiongeo_countrycode IS 'ISO country code of event location';
COMMENT ON COLUMN raw_gdelt_events.sourceurl IS 'URL of source article';
COMMENT ON COLUMN raw_gdelt_events.ingested_at IS 'Timestamp when record was ingested into warehouse';
