-- Snowflake warehouse layers for the customer CDC pipeline.
--   RAW        landing/staging of parsed Debezium events (see staging.sql)
--   CORE       CUSTOMER_HISTORY, the SCD Type 2 dimension
--   ANALYTICS  consumer-facing views (see analytics_views.sql)
-- Executed by `cdc-warehouse init` against the database/role in .env.

CREATE SCHEMA IF NOT EXISTS RAW;
CREATE SCHEMA IF NOT EXISTS CORE;
CREATE SCHEMA IF NOT EXISTS ANALYTICS;

CREATE TABLE IF NOT EXISTS CORE.CUSTOMER_HISTORY (
    CUSTOMER_VERSION_KEY  VARCHAR       NOT NULL,   -- customer_id || '-' || source_lsn
    CUSTOMER_ID           NUMBER(38,0)  NOT NULL,
    FIRST_NAME            VARCHAR,
    LAST_NAME             VARCHAR,
    EMAIL                 VARCHAR,
    PHONE                 VARCHAR,
    ADDRESS               VARCHAR,
    CITY                  VARCHAR,
    STATE                 VARCHAR,
    CUSTOMER_STATUS       VARCHAR,
    SOURCE_UPDATED_AT     TIMESTAMP_NTZ,
    RECORD_HASH           VARCHAR       NOT NULL,   -- MD5 of tracked attributes
    IS_DELETED            BOOLEAN       NOT NULL,
    VALID_FROM            TIMESTAMP_NTZ NOT NULL,   -- source commit time (UTC)
    VALID_TO              TIMESTAMP_NTZ,            -- NULL while current
    IS_CURRENT            BOOLEAN       NOT NULL,
    SOURCE_LSN            NUMBER(38,0)  NOT NULL,
    SOURCE_OP             VARCHAR(1)    NOT NULL,
    CREATED_AT            TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT            TIMESTAMP_NTZ,
    CONSTRAINT PK_CUSTOMER_HISTORY PRIMARY KEY (CUSTOMER_VERSION_KEY)
)
CLUSTER BY (CUSTOMER_ID);
