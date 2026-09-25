-- RAW/STAGING: one row per Debezium change event, flattened by the Python
-- consumer. Rows are append-only; PROCESSED_AT is stamped by scd_type_2.sql
-- once an event has been applied to CORE.CUSTOMER_HISTORY.
-- CHANGE_KEY (customer_id|lsn|op) is the idempotency key: the consumer only
-- inserts keys that are not already present, so Kafka redeliveries
-- (at-least-once) never produce duplicate staged rows.

CREATE TABLE IF NOT EXISTS RAW.CUSTOMER_CDC_EVENTS (
    CHANGE_KEY         VARCHAR       NOT NULL,
    CUSTOMER_ID        NUMBER(38,0)  NOT NULL,
    OP                 VARCHAR(1)    NOT NULL,   -- c=create u=update d=delete r=snapshot read
    IS_DELETE          BOOLEAN       NOT NULL,
    FIRST_NAME         VARCHAR,
    LAST_NAME          VARCHAR,
    EMAIL              VARCHAR,
    PHONE              VARCHAR,
    ADDRESS            VARCHAR,
    CITY               VARCHAR,
    STATE              VARCHAR,
    CUSTOMER_STATUS    VARCHAR,
    SOURCE_UPDATED_AT  TIMESTAMP_NTZ,
    RECORD_HASH        VARCHAR       NOT NULL,
    SOURCE_LSN         NUMBER(38,0)  NOT NULL,
    SOURCE_TX_ID       NUMBER(38,0),
    EVENT_TS           TIMESTAMP_NTZ NOT NULL,   -- source.ts_ms (commit time, UTC)
    KAFKA_TOPIC        VARCHAR,
    KAFKA_PARTITION    NUMBER(10,0),
    KAFKA_OFFSET       NUMBER(38,0),
    RAW_PAYLOAD        VARIANT,
    LOADED_AT          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PROCESSED_AT       TIMESTAMP_NTZ,
    CONSTRAINT PK_CUSTOMER_CDC_EVENTS PRIMARY KEY (CHANGE_KEY)
);
