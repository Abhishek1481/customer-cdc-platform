# Customer Data Change Tracking Platform

[![ci](../../actions/workflows/ci.yml/badge.svg)](../../actions/workflows/ci.yml)

A change data capture (CDC) pipeline. Every INSERT, UPDATE and DELETE on a PostgreSQL `customers` table is read from the write-ahead log by **Debezium**, published to **Kafka**, consumed by a **Python** loader, and stored as a full **SCD Type 2** history in **Snowflake**. A local DuckDB warehouse stands in for Snowflake during development.

> **Status: local development / demonstration environment.** The Snowflake SQL and adapter are implemented and unit-tested against a mocked connector, but they have **not** been run against a real Snowflake account. The Docker Compose stack is written but could not be started on the build machine (Docker wasn't installed). The parts that were actually run are listed under [Testing](#9-testing).

---

## 1. Project overview

Nightly batch ETL copies whatever the `customers` table looks like at that moment. That causes four problems:

1. Large analytical `SELECT`s run against the production database.
2. A customer who changes their address twice between runs only shows up once. The middle state is lost.
3. Nobody can answer "what was this customer's status on 3 March?"
4. Analysts see changes hours late.

This project reads changes from the database's own transaction log instead of querying tables. Every committed change becomes an event, and the warehouse keeps every version of every customer.

## 2. Business value

- **Nothing is missed.** Every committed change is captured, including intermediate states and deletes.
- **Point-in-time answers.** `valid_from` / `valid_to` let you rebuild the customer base as of any moment, for audits, churn analysis and support investigations.
- **No extra query load on production.** Debezium reads the WAL. It doesn't run analytical queries against the OLTP tables.
- **Near real time.** Latency is seconds to minutes (the micro-batch interval), not a nightly window.
- **Deletes are kept.** A deleted customer keeps their history and gets a tombstone version, so they don't silently disappear.

## 3. Architecture

```mermaid
flowchart LR
    subgraph Source["Operational system"]
        APP["cdc-workload<br/>(simulated app)"] -->|INSERT/UPDATE/DELETE| PG[("PostgreSQL 16<br/>wal_level=logical")]
    end
    PG -->|"pgoutput + publication<br/>(replication slot)"| DBZ["Debezium<br/>PostgreSQL connector<br/>(Kafka Connect)"]
    DBZ -->|"key = customer_id"| T[["Kafka topic<br/>cdc.public.customers<br/>3 partitions"]]
    T --> C["Python consumer<br/>cdc-consumer"]
    C -->|malformed| DLQ[["cdc.public.customers.dlq"]]
    C -->|"batch insert,<br/>dedupe on change_key"| RAW[("RAW.CUSTOMER_CDC_EVENTS")]
    RAW -->|"scd_type_2.sql<br/>(ordered by LSN)"| CORE[("CORE.CUSTOMER_HISTORY<br/>SCD Type 2")]
    CORE --> AN[("ANALYTICS views<br/>customer_current<br/>customer_version_summary")]
    subgraph WH["Snowflake (or DuckDB locally)"]
        RAW
        CORE
        AN
    end
```

## 4. Technology stack

| Technology | Purpose |
|---|---|
| PostgreSQL 16 | Source OLTP database. `wal_level=logical`, `REPLICA IDENTITY FULL`, and a publication for the table |
| Debezium 3.6 (Kafka Connect) | Reads the WAL through the `pgoutput` plugin and emits one event per row change |
| Apache Kafka 4.3 (KRaft) | Durable, ordered, replayable event log between the source and the warehouse |
| Python 3.11 + confluent-kafka | Consumer: parsing, validation, dead-lettering, retries, batching, offset management |
| Snowflake | Target warehouse (RAW, CORE, ANALYTICS layers) |
| DuckDB | Local development warehouse. Runs the **same** SCD2 SQL file as Snowflake |
| tenacity | Exponential backoff with jitter for transient failures |
| Docker Compose | Local stack with health checks and ordered start-up |
| pytest, ruff, mypy (strict), uv | Tests, linting and formatting, type checking, dependency locking |

## 5. Data flow

1. **Change.** The application (simulated by `cdc-workload`) commits `UPDATE customers SET city='Dallas' WHERE customer_id=1`.
2. **WAL.** PostgreSQL writes the change to the WAL. Because of `REPLICA IDENTITY FULL`, the record carries the complete old row as well as the new one.
3. **Capture.** Debezium holds a logical replication slot (`customers_cdc_slot`) on the publication `customers_publication`. It decodes the change and produces this event:

   ```json
   {"before": {"customer_id": 1, "city": "Austin", ...},
    "after":  {"customer_id": 1, "city": "Dallas", ...},
    "source": {"lsn": 24000640, "txId": 2400064, "ts_ms": 1717236360000, "table": "customers", ...},
    "op": "u", "ts_ms": 1717236360150}
   ```

   `op` is `c` (create), `u` (update), `d` (delete) or `r` (snapshot read). After a delete, Debezium also sends a *tombstone* (null value) so log compaction can remove the key.
4. **Kafka.** The message key is `{"customer_id": 1}`, so all events for a customer go to the same partition and stay in order.
5. **Consume.** `cdc-consumer` polls up to `CONSUMER_BATCH_SIZE` messages. It parses each one into a typed `ChangeEvent`, sends unparseable messages to the DLQ with error headers, and skips tombstones.
6. **Stage.** The batch is inserted into `RAW.CUSTOMER_CDC_EVENTS`. Any row whose `change_key` (`customer_id|lsn|op`) is already present is skipped. **Only after that does the consumer commit Kafka offsets.**
7. **SCD Type 2.** Every `SCD_INTERVAL_SECONDS`, [`snowflake/scd_type_2.sql`](snowflake/scd_type_2.sql):
   - orders pending events by LSN
   - drops no-op changes, where the attribute hash is unchanged
   - closes the current version (`is_current=FALSE`, `valid_to=<change time>`)
   - inserts new versions (`is_current=TRUE`, `valid_to=NULL`)
   - marks the staged events as processed

   The DML runs in one transaction.
8. **Analytics.** Views in `ANALYTICS` expose the current state and change summaries. See [`snowflake/analytics_queries.sql`](snowflake/analytics_queries.sql).

### SCD Type 2 rules

| Event | Effect on `CORE.CUSTOMER_HISTORY` |
|---|---|
| INSERT / snapshot | New version: `is_current=TRUE`, `valid_from=<commit ts>`, `valid_to=NULL` |
| UPDATE (tracked column changed) | Old version: `is_current=FALSE`, `valid_to=<commit ts>`. New version is current from that time. |
| UPDATE (only `updated_at` changed) | No new version: `record_hash` is unchanged |
| DELETE | Current version closed. A **tombstone version** is inserted with `is_deleted=TRUE`, `is_current=TRUE` and the last known attributes. History is never physically deleted. |
| Re-INSERT of a deleted id | The tombstone is closed and a new live version is opened |
| Replayed / duplicate event | Ignored: `change_key` dedupe at staging, and LSN ≤ the current version's LSN at merge |

`valid_from` and `valid_to` use the **source commit time** (`source.ts_ms`), not load time. Re-running the pipeline therefore gives identical history, and "as of" queries reflect when things actually happened.

## 6. Installation

Prerequisites: [uv](https://docs.astral.sh/uv/) for local development. [Docker Desktop](https://www.docker.com/products/docker-desktop/) for the full stack.

```bash
git clone https://github.com/<your-username>/customer-cdc-platform.git
cd customer-cdc-platform
uv sync
```

`uv sync` installs Python 3.11 (pinned in `.python-version`) and all locked dependencies. For Snowflake, add `uv sync --extra snowflake`.

## 7. Configuration

All settings come from environment variables. Copy the template and edit it:

```bash
cp .env.example .env
```

| Variable | Meaning |
|---|---|
| `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB` | Source database owner (created by Compose) |
| `DEBEZIUM_DB_USER`, `DEBEZIUM_DB_PASSWORD` | Least-privilege `REPLICATION` login that Debezium uses |
| `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_GROUP_ID` | Kafka connection and consumer group |
| `CONSUMER_BATCH_SIZE`, `CONSUMER_BATCH_TIMEOUT_SECONDS` | Micro-batch size and latency trade-off |
| `SCD_INTERVAL_SECONDS` | How often staged events are merged into history |
| `WAREHOUSE_TARGET` | `duckdb` (local) or `snowflake` |
| `SNOWFLAKE_*` | Account, user, role, warehouse, database. Key-pair auth via `SNOWFLAKE_PRIVATE_KEY_PATH` is preferred over `SNOWFLAKE_PASSWORD`. |
| `LOG_LEVEL`, `LOG_FORMAT` | `INFO` / `DEBUG`, and `json` / `text` |

How secrets are handled:

- `.env` is git-ignored.
- The Debezium connector config references `${env:DEBEZIUM_DB_PASSWORD}` through Kafka Connect's `EnvVarConfigProvider`, so the password isn't in the JSON or the Connect config topic.
- The settings dataclasses exclude secrets from `repr()`, so they don't show up in logs.

## 8. Running the project

### A. Local mode (no Docker, no accounts)

This replays Debezium-format events from a file through the same parse → stage → SCD2 code the Kafka consumer uses:

```bash
uv run cdc-replay sample_events/customer_changes.jsonl
uv run cdc-warehouse queries                      # list available queries
uv run cdc-warehouse query current_customers
uv run cdc-warehouse query customer_history
uv run cdc-warehouse query changed_customers
uv run cdc-warehouse query versions_per_customer
```

The warehouse file defaults to `data/warehouse.duckdb` (git-ignored). Set `DUCKDB_PATH` to change it.

### B. Full CDC stack (Docker)

> Written but **not yet run** on the build machine: Docker wasn't available there. The image tags were checked against the registries on 2026-09-24.

```bash
cp .env.example .env              # set the three passwords
docker compose up -d --build      # postgres, kafka, connect, bootstrap (topics + connector), consumer
docker compose ps                 # bootstrap should be "exited (0)"; the others healthy/running
curl -s localhost:8083/connectors/customers-postgres-cdc/status

# Generate changes in PostgreSQL (the "application")
docker compose --profile tools run --rm workload run --inserts 5 --updates 20 --deletes 2
docker compose --profile tools run --rm workload lifecycle

# Inspect results (the consumer applies SCD2 every SCD_INTERVAL_SECONDS)
docker compose run --rm consumer cdc-warehouse query customer_history
docker compose logs -f consumer
```

Start-up order is enforced by health checks: `postgres` and `kafka` become healthy, then `connect`, then the one-shot `bootstrap`. `bootstrap` creates the topics from `kafka/topics.json` and PUTs `debezium/connector-config.json`, then waits for `RUNNING`. The `consumer` starts only after `bootstrap` exits successfully.

### C. Snowflake

1. An administrator runs [`snowflake/setup_account.sql`](snowflake/setup_account.sql) once. It creates the warehouse, database and `CDC_LOADER` role.
2. Set `WAREHOUSE_TARGET=snowflake` and the `SNOWFLAKE_*` variables in `.env`.
3. Run `uv sync --extra snowflake`, then `uv run --env-file .env cdc-warehouse init`. This creates `RAW`, `CORE`, `ANALYTICS`, the tables and the views.
4. Start the consumer (Compose, or `uv run --env-file .env cdc-consumer`).

**This path is untested:** no Snowflake account was available. The adapter's SQL generation, statement ordering (DDL before `BEGIN`) and error mapping are covered by unit tests against a fake connector.

## 9. Testing

```bash
uv run pytest                     # unit tests; integration tests auto-skip
uv run ruff check . && uv run ruff format --check .
uv run mypy                       # strict mode over src/
```

| Suite | What it covers | Where it ran |
|---|---|---|
| `tests/unit/test_events.py` | Debezium envelope parsing, all op types, schema-wrapped payloads, 10 malformed-input cases, timestamps, record hash | Local ✅ |
| `tests/unit/test_scd_type_2.py` | The **real** `scd_type_2.sql` on DuckDB: insert, update, multi-change batches arriving out of order, delete tombstones, re-insert, no-op updates, duplicate delivery, replays, a single current row per customer, every analytics query | Local ✅ |
| `tests/unit/test_consumer.py` | Batch processing, DLQ routing, retry and give-up, commit only after staging, fatal Kafka errors, graceful stop | Local ✅ |
| `tests/unit/test_snowflake_warehouse.py` | Snowflake adapter against a fake connector: DDL, statement order, retryable errors, secrets not in repr | Local ✅ |
| `tests/integration/test_postgres_source.py` (`TEST_POSTGRES_DSN`) | `init.sql` + `seed.sql`, trigger, check constraint, workload generator, and **reading the WAL through `pgoutput` + the publication** to prove a DELETE carries the full old row | Local ✅ against PostgreSQL 16.2 with `wal_level=logical` |
| `tests/integration/test_kafka_consumer.py` (`TEST_KAFKA_BOOTSTRAP`) | Real broker: produce → consume → stage → SCD2, committed offsets, DLQ message and headers | CI only (Kafka service container). Not run locally. |

Latest local run: `55 passed, 6 skipped` for unit tests, and `5 passed, 1 skipped` for Postgres integration.

To run the integration suites yourself:

```bash
TEST_POSTGRES_DSN=postgresql://user:pass@localhost:5432/postgres uv run pytest tests/integration
TEST_KAFKA_BOOTSTRAP=localhost:9092 uv run pytest tests/integration
```

## 10. Example output

[`docs/example-output.md`](docs/example-output.md) holds output **captured** from the local replay mode. An excerpt showing customer 1's history:

```text
customer_id | customer_status | email                    | city   | is_deleted | valid_from          | valid_to            | is_current | source_op
1           | ACTIVE          | maria.garcia@example.com | Austin | False      | 2024-06-01 10:01:00 | 2024-06-01 10:06:00 | False      | r
1           | ACTIVE          | maria.garcia@example.com | Dallas | False      | 2024-06-01 10:06:00 | 2024-06-01 10:08:00 | False      | u
1           | ACTIVE          | maria.g@example.com      | Dallas | False      | 2024-06-01 10:08:00 |                     | True       | u
```

From the same run: 14 messages received, 1 tombstone skipped, 1 malformed message dead-lettered, 1 duplicate skipped at staging, 11 events staged. Those 11 events produced 10 versions; one "touch" update created no version.

## 11. Design decisions

- **Why CDC instead of batch ETL?** Batch polling misses intermediate states and deletes, and it puts analytical load on production. Log-based CDC captures every committed change with low source overhead.
- **Why Debezium?** It is the standard open-source log-based CDC connector. It handles the initial snapshot, keeps its WAL position (LSN) in Kafka Connect offsets, and emits a consistent envelope with before/after images. Writing a custom `pgoutput` client would mean re-implementing all of that.
- **Why `pgoutput`?** It is built into PostgreSQL 10+, so the database needs no extra plugin.
- **Why `REPLICA IDENTITY FULL`?** Without it, a DELETE event only carries the primary key, and the tombstone version couldn't record the last known attributes. The cost is more WAL per update or delete. For a very large, hot table you would weigh that cost.
- **Why Kafka?** It decouples the source from the warehouse. Snowflake can be down for an hour and nothing is lost: events wait in the topic, retained for 7 days. Kafka also allows replay and gives per-key ordering through partitioning.
- **Why a Python consumer rather than the Snowflake Kafka connector?** The consumer shows the processing concerns explicitly: validation, DLQ, retries, idempotency and commit semantics. The Snowflake Kafka Connector with Snowpipe Streaming is a valid production alternative for the RAW load. The SCD2 SQL would stay the same.
- **Why SCD Type 2?** It's the standard dimensional technique for full attribute history with point-in-time queries. Type 1 overwrites history. Type 4 or 6 add complexity this use case doesn't need.
- **Why ELT for the SCD step?** Raw events land first and are auditable and replayable. The history is then built in SQL inside the warehouse, where set-based operations scale.
- **Why DuckDB locally?** It runs in-process, so no server is needed, and its dialect overlaps Snowflake enough (`QUALIFY`, window functions, `UPDATE … FROM`, `CREATE OR REPLACE TEMPORARY TABLE`) that `scd_type_2.sql`, `analytics_views.sql` and `analytics_queries.sql` run **unchanged** on both engines. Only DDL types differ.
- **Why source commit time for `valid_from`?** It is deterministic. Replaying last week's events reproduces identical history.

## 12. Failure handling

| Failure | Behavior |
|---|---|
| Malformed event (bad JSON, unknown op, missing image, wrong types) | Sent to `cdc.public.customers.dlq` with the original bytes and headers `dlq.error`, `dlq.source.topic/partition/offset`, `dlq.failed_at`. Processing continues. |
| DLQ write fails | `flush()` raises, so offsets are **not** committed and the batch is redelivered. A message is never dropped silently. |
| Warehouse transient error (lock, network, Snowflake `OperationalError`) | Retried with exponential backoff and jitter (`RETRY_MAX_ATTEMPTS`). If still failing, the process exits non-zero **without committing**. Compose restarts it, Kafka redelivers, and dedupe makes that safe. |
| Crash between staging and offset commit | The batch is redelivered, and `change_key` dedupe makes the second insert a no-op (**at-least-once delivery, effectively-once result**) |
| Crash during SCD merge | The DML is one transaction. Staged rows stay `processed_at IS NULL` and are merged on the next run, which also runs at start-up. |
| Kafka unavailable | librdkafka retries internally. Fatal errors stop the process. PostgreSQL keeps the WAL for the replication slot, so Debezium resumes from its stored LSN. **Watch slot lag**: an abandoned slot makes the WAL grow without bound. |
| Warehouse down for a long time | Events wait in Kafka for up to the 7-day retention. Consumer lag grows but no data is lost within retention. |
| Out-of-order arrival | History is ordered by LSN, not arrival. Events older than the current version's LSN are ignored. |
| Tombstones, truncate, logical messages | Counted and skipped |
| SIGINT / SIGTERM | The current batch finishes, a final SCD merge runs, then offsets are committed and the consumer closes cleanly |

## 13. Scalability

No benchmarks were run. This section is a design discussion, not measured results.

- **~1K records:** the current setup as is. One consumer, one partition would be enough, and DuckDB works fine.
- **~1M records / steady change stream:**
  - Use 3–12 partitions and scale consumers in the same group up to the partition count. The key is `customer_id`, so per-customer ordering holds.
  - Increase `CONSUMER_BATCH_SIZE`.
  - Load RAW with Snowflake bulk paths. The connector's `executemany` already batch-binds; the next step is `write_pandas`/`COPY INTO` from a stage.
  - Cluster `CUSTOMER_HISTORY` by `customer_id` (already declared).
  - Make the SCD job a scheduled Snowflake TASK over a STREAM on the RAW table.
- **~100M+ records / high change rate:**
  - Replace the Python RAW loader with the Snowflake Kafka connector (Snowpipe Streaming).
  - Keep the SCD2 merge as incremental SQL over a STREAM, and run it on a dedicated warehouse.
  - Partition the RAW table by load date and prune it.
  - Use Debezium's incremental snapshots (signal table) for re-snapshots without long locks.
  - Consider dropping `REPLICA IDENTITY FULL` in favor of key-only deletes plus lookups, to cut WAL volume.
  - Monitor replication-slot lag, consumer lag and DLQ volume.

## Repository layout

```text
customer-cdc-platform/
├── src/customer_cdc/
│   ├── producer/          workload generator (simulated application writes)
│   ├── consumer/          Debezium parsing, batch processor, Kafka loop, DLQ, replay mode
│   ├── transformations/   cdc-warehouse CLI (init / scd / query)
│   ├── warehouse/         DuckDB + Snowflake adapters (shared staging and SCD code)
│   └── utils/             config, structured logging, retry, SQL helpers, bootstrap
├── postgres/              init.sql (schema, trigger, publication), seed.sql, replication user
├── debezium/              connector-config.json
├── kafka/                 topics.json (partitions, retention, DLQ)
├── snowflake/             setup_account.sql, schema.sql, staging.sql, scd_type_2.sql,
│                          analytics_views.sql, analytics_queries.sql
├── sample_events/         synthetic Debezium-format fixture for local mode
├── tests/                 unit + integration tests
├── docs/                  interview notes, CDC concepts, captured example output
├── docker-compose.yml, Dockerfile, .env.example, pyproject.toml, uv.lock
```

Python code lives in one `customer_cdc` package with `producer/`, `consumer/`, `transformations/` and `utils/` sub-packages, rather than as top-level `src/producer` directories. The generic top-level names (`utils`, `consumer`) could clash with other installed packages.

Resume bullets: [docs/resume-bullets.md](docs/resume-bullets.md) · More reading: [docs/cdc-concepts.md](docs/cdc-concepts.md) (WAL, Debezium events, offsets, Kafka terms) · [docs/interview-notes.md](docs/interview-notes.md)
