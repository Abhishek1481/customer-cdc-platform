"""``cdc-warehouse``: initialise the warehouse, apply SCD Type 2, run analytics queries.

cdc-warehouse init            create RAW / CORE / ANALYTICS objects
cdc-warehouse scd             apply staged events to CORE.CUSTOMER_HISTORY
cdc-warehouse queries         list the named analytics queries
cdc-warehouse query <name>    run one named query and print a table
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from typing import Any

from customer_cdc.utils.config import RetrySettings
from customer_cdc.utils.logging_setup import configure_logging
from customer_cdc.utils.retry import call_with_retry
from customer_cdc.warehouse import create_warehouse


def format_table(columns: Sequence[str], rows: Sequence[Sequence[Any]]) -> str:
    cells = [[("" if v is None else str(v)) for v in row] for row in rows]
    widths = [max([len(c)] + [len(r[i]) for r in cells]) for i, c in enumerate(columns)]
    line = " | ".join(c.ljust(w) for c, w in zip(columns, widths, strict=True))
    sep = "-+-".join("-" * w for w in widths)
    body = [" | ".join(v.ljust(w) for v, w in zip(r, widths, strict=True)) for r in cells]
    return "\n".join([line, sep, *body, f"({len(rows)} rows)"])


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("init")
    sub.add_parser("scd")
    sub.add_parser("queries")
    q = sub.add_parser("query")
    q.add_argument("name")
    args = parser.parse_args(argv)

    configure_logging()
    warehouse = create_warehouse()
    retry = RetrySettings.from_env()
    try:
        if args.command == "init":
            call_with_retry(warehouse.initialize, retry)
        elif args.command == "scd":
            result = call_with_retry(warehouse.apply_scd, retry)
            print(
                f"events_processed={result.events_processed} "
                f"versions_inserted={result.versions_inserted}"
            )
        elif args.command == "queries":
            print("\n".join(sorted(warehouse.named_queries())))
        else:
            queries = warehouse.named_queries()
            if args.name not in queries:
                print(
                    f"unknown query {args.name!r}; choose from {sorted(queries)}", file=sys.stderr
                )
                return 2
            columns, rows = call_with_retry(lambda: warehouse.query(queries[args.name]), retry)
            print(format_table(columns, rows))
    finally:
        warehouse.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
