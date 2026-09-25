-- Local development warehouse (DuckDB) equivalent of snowflake/schema.sql and
-- snowflake/staging.sql. Only the DDL differs; the SCD Type 2 SQL and the
-- analytics views/queries are shared verbatim with Snowflake.

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS raw.customer_cdc_events (
    change_key         VARCHAR   NOT NULL PRIMARY KEY,
    customer_id        BIGINT    NOT NULL,
    op                 VARCHAR   NOT NULL,
    is_delete          BOOLEAN   NOT NULL,
    first_name         VARCHAR,
    last_name          VARCHAR,
    email              VARCHAR,
    phone              VARCHAR,
    address            VARCHAR,
    city               VARCHAR,
    state              VARCHAR,
    customer_status    VARCHAR,
    source_updated_at  TIMESTAMP,
    record_hash        VARCHAR   NOT NULL,
    source_lsn         BIGINT    NOT NULL,
    source_tx_id       BIGINT,
    event_ts           TIMESTAMP NOT NULL,
    kafka_topic        VARCHAR,
    kafka_partition    INTEGER,
    kafka_offset       BIGINT,
    raw_payload        JSON,
    loaded_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processed_at       TIMESTAMP
);

CREATE TABLE IF NOT EXISTS core.customer_history (
    customer_version_key  VARCHAR   NOT NULL PRIMARY KEY,
    customer_id           BIGINT    NOT NULL,
    first_name            VARCHAR,
    last_name             VARCHAR,
    email                 VARCHAR,
    phone                 VARCHAR,
    address               VARCHAR,
    city                  VARCHAR,
    state                 VARCHAR,
    customer_status       VARCHAR,
    source_updated_at     TIMESTAMP,
    record_hash           VARCHAR   NOT NULL,
    is_deleted            BOOLEAN   NOT NULL,
    valid_from            TIMESTAMP NOT NULL,
    valid_to              TIMESTAMP,
    is_current            BOOLEAN   NOT NULL,
    source_lsn            BIGINT    NOT NULL,
    source_op             VARCHAR   NOT NULL,
    created_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at            TIMESTAMP
);
