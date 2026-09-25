"""Helpers for executing SQL files statement by statement."""

from __future__ import annotations

import re
from pathlib import Path

_NAME_MARKER = re.compile(r"^--\s*name:\s*(?P<name>[A-Za-z0-9_]+)\s*$", re.MULTILINE)


def strip_comments(sql: str) -> str:
    """Remove ``--`` line comments that are outside string literals."""
    out: list[str] = []
    in_string = False
    i = 0
    while i < len(sql):
        ch = sql[i]
        if ch == "'":
            in_string = not in_string
            out.append(ch)
        elif not in_string and sql.startswith("--", i):
            newline = sql.find("\n", i)
            if newline == -1:
                break
            i = newline
            continue
        else:
            out.append(ch)
        i += 1
    return "".join(out)


def split_statements(sql: str) -> list[str]:
    """Split a script on semicolons that are not inside string literals."""
    statements: list[str] = []
    current: list[str] = []
    in_string = False
    for ch in strip_comments(sql):
        if ch == "'":
            in_string = not in_string
        if ch == ";" and not in_string:
            stmt = "".join(current).strip()
            if stmt:
                statements.append(stmt)
            current = []
        else:
            current.append(ch)
    tail = "".join(current).strip()
    if tail:
        statements.append(tail)
    return statements


def load_named_queries(path: Path) -> dict[str, str]:
    """Parse a file of ``-- name: <query>`` sections into {name: sql}."""
    text = path.read_text(encoding="utf-8")
    matches = list(_NAME_MARKER.finditer(text))
    queries: dict[str, str] = {}
    for idx, match in enumerate(matches):
        end = matches[idx + 1].start() if idx + 1 < len(matches) else len(text)
        body = split_statements(text[match.end() : end])
        if body:
            queries[match.group("name")] = body[0]
    return queries
