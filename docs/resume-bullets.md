# Resume bullets: Customer Data Change Tracking Platform

Every bullet describes something implemented and tested in this repository. There are **no invented performance numbers**: no benchmarks were run. The only figures come from runs recorded in this project's `docs/example-output.md`, and they describe small demo runs, not production workloads. Label these as personal/portfolio projects, and adjust the tense and wording to your own resume style.

## Customer Data Change Tracking Platform (PostgreSQL, Debezium, Kafka, Python, Snowflake, Docker)

- Built a log-based change data capture pipeline streaming PostgreSQL INSERT/UPDATE/DELETE events through Debezium (`pgoutput`) and Apache Kafka into a Snowflake-compatible warehouse as an alternative to batch polling of the source database.
- Implemented a Python Kafka consumer with manual offset commits after durable staging, a dead-letter topic for malformed events, exponential-backoff retries and graceful shutdown. This gives at-least-once delivery with idempotent, effectively-once warehouse results.
- Designed an SCD Type 2 customer history model in SQL. It orders changes by WAL LSN, suppresses no-op updates via attribute hashing, preserves deletes as tombstone versions, and ignores replayed events. The same SQL runs on Snowflake and on DuckDB for local development.
- Configured PostgreSQL for logical replication (publication, `REPLICA IDENTITY FULL`, least-privilege replication role). Verified full before-images for deletes with an integration test that decodes the `pgoutput` WAL stream.
- Containerized the stack with Docker Compose (PostgreSQL, Kafka KRaft, Debezium Connect, consumer) using health-checked start-up ordering and environment-based secret injection through Kafka Connect config providers.
