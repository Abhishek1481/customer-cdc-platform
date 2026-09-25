# Sample Debezium events

`customer_changes.jsonl` is a **synthetic** fixture, hand-built with
`tests/factories.py` to follow the Debezium PostgreSQL envelope produced by the
JSON converter with `schemas.enable=false`. It was not captured from a live
connector. One message per line:

| Lines | Scenario |
|---|---|
| 1-4 | initial snapshot (`op=r`) of customers 1-4 |
| 5 | insert of customer 5 (`op=c`) |
| 6, 8 | customer 1 moves city, then changes email |
| 7, 12 | customer 2 suspended, then re-activated |
| 9 | "touch" update of customer 3 (only `updated_at` changes -> no new SCD2 version) |
| 10, 11 | delete of customer 4 followed by its tombstone (`null`) |
| 13 | a malformed message (goes to the dead-letter path) |
| 14 | duplicate delivery of line 6 (deduplicated by `change_key`) |

Replay it through the full downstream pipeline with:

```bash
uv run cdc-replay sample_events/customer_changes.jsonl
```
