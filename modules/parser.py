from __future__ import annotations

import re
from typing import Generator, Iterable, Optional, Tuple

from .types import ImportOptions, ParseError


def is_insert_or_replace(statement: str) -> bool:
    # This code here checks if the statement looks like an INSERT or REPLACE.
    s = statement.lstrip().upper()
    return s.startswith("INSERT ") or s.startswith("REPLACE ")


def is_insert_or_replace_or_update(statement: str) -> bool:
    # This code here is a wider net for INSERT/REPLACE variants.
    s = statement.lstrip().upper()
    return (
        s.startswith("INSERT ")
        or s.startswith("REPLACE ")
        or s.startswith("INSERT OR REPLACE ")
    )


def maybe_transform_statement(statement: str, opts: ImportOptions) -> Optional[str]:
    # This code here does cleanup and light transforms to keep MySQL happy.
    if opts.no_transforms:
        return statement

    stripped = statement.lstrip()
    upper = stripped.upper()

    if upper.startswith("PRAGMA "):
        # SQLite-only stuff, not invited to this party.
        return None

    if upper.startswith("BEGIN TRANSACTION") or upper.startswith("COMMIT"):
        # We manage our own transactions; thanks though.
        return None

    if opts.ignore_locks and (
        upper.startswith("LOCK TABLES") or upper.startswith("UNLOCK TABLES")
    ):
        # Locks from the dump can just be skipped.
        return None

    if upper.startswith("DELIMITER "):
        # Routines need delimiter handling; we either skip or fail fast.
        if opts.allow_delimiter:
            return None
        raise ParseError("DELIMITER directives are not supported without --allow-delimiter")

    if opts.transform_insert_or_replace and upper.startswith("INSERT OR REPLACE "):
        # This code here swaps SQLite syntax into MySQL REPLACE.
        return "REPLACE " + stripped[len("INSERT OR REPLACE "):]

    return statement


_INSERT_TABLE_RE = re.compile(
    r"^(INSERT\s+INTO|REPLACE\s+INTO|REPLACE)\s+`?([\w$]+)`?(?:\.`?([\w$]+)`?)?",
    re.IGNORECASE,
)


def extract_insert_table(statement: str) -> Optional[str]:
    # This code here extracts the table name so we can stage per-table imports.
    s = statement.lstrip()
    match = _INSERT_TABLE_RE.match(s)
    if not match:
        return None
    schema = match.group(2)
    table = match.group(3) or match.group(2)
    if match.group(3) and schema:
        return f"{schema}.{table}"
    return table


def statement_splitter(
    stream: Iterable[str],
) -> Generator[Tuple[str, int, int], None, None]:
    """
    Split statements on semicolons not inside quotes/backticks.
    Yields (statement, start_line, end_line).
    """
    # This code here is the heart of streaming: one statement at a time, no big memory spike.
    buf: list[str] = []
    line_no = 0
    stmt_start = 1

    in_single = False
    in_double = False
    in_backtick = False
    in_line_comment = False
    in_block_comment = False

    for line in stream:
        line_no += 1
        i = 0
        length = len(line)

        while i < length:
            ch = line[i]
            nxt = line[i + 1] if i + 1 < length else ""

            if in_line_comment:
                buf.append(ch)
                i += 1
                continue

            if in_block_comment:
                buf.append(ch)
                if ch == "*" and nxt == "/":
                    buf.append(nxt)
                    i += 2
                    in_block_comment = False
                    continue
                i += 1
                continue

            if not (in_single or in_double or in_backtick):
                if ch == "-" and nxt == "-":
                    in_line_comment = True
                elif ch == "#":
                    in_line_comment = True
                elif ch == "/" and nxt == "*":
                    in_block_comment = True

            if ch == "'" and not (in_double or in_backtick):
                if in_single and nxt == "'":
                    buf.append(ch)
                    buf.append(nxt)
                    i += 2
                    continue
                if not (i > 0 and line[i - 1] == "\\"):
                    in_single = not in_single
            elif ch == '"' and not (in_single or in_backtick):
                if in_double and nxt == '"':
                    buf.append(ch)
                    buf.append(nxt)
                    i += 2
                    continue
                if not (i > 0 and line[i - 1] == "\\"):
                    in_double = not in_double
            elif ch == "`" and not (in_single or in_double):
                in_backtick = not in_backtick

            buf.append(ch)

            if (
                ch == ";"
                and not (in_single or in_double or in_backtick)
                and not (in_line_comment or in_block_comment)
            ):
                statement = "".join(buf).strip()
                buf = []
                end_line = line_no
                if statement:
                    yield statement, stmt_start, end_line
                stmt_start = line_no
            i += 1

        if in_line_comment:
            in_line_comment = False

    tail = "".join(buf).strip()
    if tail:
        yield tail, stmt_start, line_no
