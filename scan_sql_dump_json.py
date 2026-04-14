#!/usr/bin/env python3
"""Scan SQL dump INSERT payloads for invalid JSON in likely JSON columns."""

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import asdict, dataclass
from typing import Iterable

from modules.parser import extract_insert_table, statement_splitter

_INSERT_VALUES_PREFIX_RE = re.compile(
    r"^\s*(?:INSERT\s+OR\s+REPLACE\s+INTO|INSERT\s+INTO|REPLACE\s+INTO|REPLACE)\s+"
    r"(?P<table>[^\s(]+)\s*"
    r"\((?P<columns>.*?)\)\s*VALUES\s*(?P<values>.+?)\s*;?\s*$",
    re.IGNORECASE | re.DOTALL,
)

_JSON_COLUMN_HINTS = (
    "data",
    "json",
    "model",
    "payload",
    "definition",
    "condition",
    "settings",
)


@dataclass(frozen=True)
class JsonFinding:
    severity: str
    code: str
    table: str
    column: str
    start_line: int
    end_line: int
    detail: str
    recommendation: str
    snippet: str


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Scan INSERT payloads for invalid JSON in likely JSON columns."
    )
    parser.add_argument("--dump-file", required=True, help="Path to .sql dump file")
    parser.add_argument(
        "--tables",
        default="",
        help="Comma-separated table names to scan; defaults to all",
    )
    parser.add_argument(
        "--columns",
        default="",
        help="Comma-separated column names to force-check as JSON",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=50,
        help="Maximum number of findings to print",
    )
    parser.add_argument("--json", action="store_true", help="Emit findings as JSON")
    return parser


def _normalize_identifier(value: str) -> str:
    value = value.strip()
    if value.startswith("`") and value.endswith("`"):
        value = value[1:-1]
    if value.startswith('"') and value.endswith('"'):
        value = value[1:-1]
    return value


def _parse_column_list(raw: str) -> list[str]:
    columns: list[str] = []
    current: list[str] = []
    in_backtick = False
    for ch in raw:
        if ch == "`":
            in_backtick = not in_backtick
            current.append(ch)
            continue
        if ch == "," and not in_backtick:
            columns.append(_normalize_identifier("".join(current)))
            current = []
            continue
        current.append(ch)
    if current:
        columns.append(_normalize_identifier("".join(current)))
    return [column for column in columns if column]


def _split_top_level_csv(raw: str) -> list[str]:
    items: list[str] = []
    current: list[str] = []
    in_single = False
    in_double = False
    in_backtick = False
    depth = 0
    i = 0
    while i < len(raw):
        ch = raw[i]
        nxt = raw[i + 1] if i + 1 < len(raw) else ""
        if ch == "'" and not (in_double or in_backtick):
            current.append(ch)
            if in_single:
                if nxt == "'":
                    current.append(nxt)
                    i += 2
                    continue
                if not _is_escaped(raw, i):
                    in_single = False
            else:
                in_single = True
            i += 1
            continue
        if ch == '"' and not (in_single or in_backtick):
            in_double = not in_double
            current.append(ch)
            i += 1
            continue
        if ch == "`" and not (in_single or in_double):
            in_backtick = not in_backtick
            current.append(ch)
            i += 1
            continue
        if not (in_single or in_double or in_backtick):
            if ch == "(":
                depth += 1
            elif ch == ")" and depth > 0:
                depth -= 1
            elif ch == "," and depth == 0:
                items.append("".join(current).strip())
                current = []
                i += 1
                continue
        current.append(ch)
        i += 1
    if current:
        items.append("".join(current).strip())
    return items


def _extract_row_groups(values_raw: str) -> list[str]:
    rows: list[str] = []
    current: list[str] = []
    in_single = False
    in_double = False
    in_backtick = False
    depth = 0
    i = 0
    while i < len(values_raw):
        ch = values_raw[i]
        nxt = values_raw[i + 1] if i + 1 < len(values_raw) else ""
        if ch == "'" and not (in_double or in_backtick):
            current.append(ch)
            if in_single:
                if nxt == "'":
                    current.append(nxt)
                    i += 2
                    continue
                if not _is_escaped(values_raw, i):
                    in_single = False
            else:
                in_single = True
            i += 1
            continue
        if ch == '"' and not (in_single or in_backtick):
            in_double = not in_double
            current.append(ch)
            i += 1
            continue
        if ch == "`" and not (in_single or in_double):
            in_backtick = not in_backtick
            current.append(ch)
            i += 1
            continue
        if not (in_single or in_double or in_backtick):
            if ch == "(":
                depth += 1
            elif ch == ")" and depth > 0:
                depth -= 1
                if depth == 0:
                    current.append(ch)
                    rows.append("".join(current).strip())
                    current = []
                    i += 1
                    while i < len(values_raw) and values_raw[i] in " \t\r\n,":
                        i += 1
                    continue
        current.append(ch)
        i += 1
    return rows


def _is_escaped(raw: str, quote_index: int) -> bool:
    backslashes = 0
    idx = quote_index - 1
    while idx >= 0 and raw[idx] == "\\":
        backslashes += 1
        idx -= 1
    return (backslashes % 2) == 1


def _decode_sql_string(token: str) -> str | None:
    token = token.strip()
    if len(token) < 2 or token[0] != "'" or token[-1] != "'":
        return None
    inner = token[1:-1]
    out: list[str] = []
    i = 0
    while i < len(inner):
        ch = inner[i]
        nxt = inner[i + 1] if i + 1 < len(inner) else ""
        if ch == "\\" and i + 1 < len(inner):
            mapping = {
                "0": "\0",
                "b": "\b",
                "n": "\n",
                "r": "\r",
                "t": "\t",
                "Z": "\x1a",
                "\\": "\\",
                "'": "'",
                '"': '"',
            }
            out.append(mapping.get(nxt, nxt))
            i += 2
            continue
        if ch == "'" and nxt == "'":
            out.append("'")
            i += 2
            continue
        out.append(ch)
        i += 1
    return "".join(out)


def _line_for_offset(statement: str, start_line: int, offset: int) -> int:
    return start_line + statement[:offset].count("\n")


def _snippet(statement: str, max_len: int = 160) -> str:
    return statement[:max_len].replace("\n", "\\n")


def _should_check_column(column: str, forced_columns: set[str]) -> bool:
    normalized = column.lower()
    if normalized in forced_columns:
        return True
    return any(hint in normalized for hint in _JSON_COLUMN_HINTS)


def _scan_statement(
    statement: str,
    start_line: int,
    end_line: int,
    forced_columns: set[str],
) -> list[JsonFinding]:
    findings: list[JsonFinding] = []
    match = _INSERT_VALUES_PREFIX_RE.match(statement)
    if not match:
        return findings

    table = extract_insert_table(statement)
    if not table:
        return findings

    columns = _parse_column_list(match.group("columns"))
    rows = _extract_row_groups(match.group("values"))
    if not columns or not rows:
        return findings

    values_offset = match.start("values")
    search_offset = values_offset
    for row in rows:
        row_values = row.strip()
        if row_values.startswith("(") and row_values.endswith(")"):
            row_values = row_values[1:-1]
        tokens = _split_top_level_csv(row_values)
        row_offset = statement.find(row, search_offset)
        if row_offset != -1:
            search_offset = row_offset + len(row)
        line_no = _line_for_offset(statement, start_line, row_offset if row_offset != -1 else 0)
        if len(tokens) != len(columns):
            findings.append(
                JsonFinding(
                    severity="medium",
                    code="column_value_count_mismatch",
                    table=table,
                    column="-",
                    start_line=line_no,
                    end_line=end_line,
                    detail=(
                        f"Parsed {len(tokens)} values for {len(columns)} columns. "
                        "This row may have SQL structure the scanner cannot safely decode."
                    ),
                    recommendation="Send me this statement if the missing object comes from this table.",
                    snippet=_snippet(row),
                )
            )
            continue
        for column, token in zip(columns, tokens):
            if not _should_check_column(column, forced_columns):
                continue
            decoded = _decode_sql_string(token)
            if decoded is None:
                continue
            stripped = decoded.strip()
            if not stripped or stripped[0] not in "[{":
                continue
            try:
                json.loads(decoded)
            except json.JSONDecodeError as err:
                findings.append(
                    JsonFinding(
                        severity="high",
                        code="invalid_json_payload",
                        table=table,
                        column=column,
                        start_line=line_no,
                        end_line=end_line,
                        detail=f"JSON parse failed: {err.msg} at char {err.pos}",
                        recommendation=(
                            "Inspect this row in the source dump and compare it against a known-good stored value."
                        ),
                        snippet=_snippet(decoded),
                    )
                )
    return findings


def scan_dump(
    dump_file: str,
    table_filter: tuple[str, ...],
    forced_columns: set[str],
) -> list[JsonFinding]:
    findings: list[JsonFinding] = []
    with open(dump_file, "r", encoding="utf-8", errors="replace") as fp:
        for statement, start_line, end_line in statement_splitter(fp):
            table = extract_insert_table(statement)
            if not table:
                continue
            short_name = table.split(".")[-1]
            if table_filter and short_name not in table_filter:
                continue
            findings.extend(_scan_statement(statement, start_line, end_line, forced_columns))
    return findings


def _render_text(findings: list[JsonFinding], limit: int) -> str:
    shown = findings[:limit]
    lines = [
        f"Findings: {len(findings)} total"
        + (f" (showing first {len(shown)})" if len(findings) > len(shown) else "")
    ]
    for finding in shown:
        lines.append(
            f"[{finding.severity}] {finding.code} lines={finding.start_line}-{finding.end_line} "
            f"table={finding.table} column={finding.column}"
        )
        lines.append(f"  detail: {finding.detail}")
        lines.append(f"  recommendation: {finding.recommendation}")
        lines.append(f"  snippet: {finding.snippet}")
    if not findings:
        lines.append("No invalid JSON payloads found in the checked columns.")
    return "\n".join(lines)


def main(argv: Iterable[str]) -> int:
    parser = build_parser()
    args = parser.parse_args(list(argv))
    table_filter = tuple(
        table.strip() for table in str(args.tables or "").split(",") if table.strip()
    )
    forced_columns = {
        column.strip().lower()
        for column in str(args.columns or "").split(",")
        if column.strip()
    }
    findings = scan_dump(args.dump_file, table_filter, forced_columns)
    if args.json:
        print(json.dumps([asdict(finding) for finding in findings[: args.limit]], indent=2))
    else:
        print(_render_text(findings, args.limit))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
