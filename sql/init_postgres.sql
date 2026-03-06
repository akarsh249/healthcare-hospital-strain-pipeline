CREATE DATABASE warehouse;

\connect warehouse;

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS bronze.ingestion_runs (
  run_id TEXT PRIMARY KEY,
  source TEXT NOT NULL,
  started_at TIMESTAMP NOT NULL,
  ended_at TIMESTAMP,
  status TEXT NOT NULL,
  records_fetched INT DEFAULT 0
);
