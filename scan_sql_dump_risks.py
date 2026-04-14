#!/usr/bin/env python3
"""Scan a SQL dump for statements likely to confuse the importer."""

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import asdict, dataclass
from typing import Iterable

from modules.parser import extract_insert_table, maybe_transform_statement, statement_splitter
from modules.types import ImportOptions

_BACKSLASH_QUOTE_RE = re.compile(r"(\\+)(['\"])")
_INSERT_OR_REPLACE_PREFIX = "INSERT OR REPLACE "
_REPLACE_PREFIX = "REPLACE "


@dataclass(frozen=True)
class Finding:
    severity: str
    code: str
    table: str | None
    start_line: int
    end_line: int
    detail: str
    recommendation: str
    snippet: str


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Scan a SQL dump for quote/backslash patterns that are risky for this importer."
    )
    parser.add_argument("--dump-file", required=True, help="Path to .sql dump file")
    parser.add_argument(
        "--tables",
        default="",
        help="Comma-separated table names to scan; defaults to all",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=50,
        help="Maximum number of findings to print",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit findings as JSON",
    )
    return parser


def _opts() -> ImportOptions:
    return ImportOptions(
        dump_file="",
        table_filter=(),
        host="127.0.0.1",
        port=3306,
        user="root",
        password="",
        target_db="grafana",
        commit_statements=500,
        commit_bytes=5 * 1024 * 1024,
        disable_foreign_keys=False,
        disable_unique_checks=False,
        sql_mode=None,
        autocommit=False,
        force_charset="utf8mb4",
        create_db=False,
        recreate_db=False,
        quarantine_file="quarantine.sql",
        quarantine_all_failures=True,
        quarantine_only_inserts=False,
        fail_on_error=False,
        progress_mb=50,
        progress_statements=5000,
        progress_bar=False,
        log_file=None,
        progress_bar_logs=False,
        worker_progress=False,
        worker_progress_interval=5.0,
        auto_tune_batch=True,
        combine_inserts=False,
        combine_insert_group_size=25,
        resume=False,
        resume_file="import.resume.json",
        ignore_locks=False,
        allow_delimiter=False,
        no_transforms=False,
        transform_insert_or_replace=True,
        parallel_per_table=False,
        parallel_workers=4,
        parallel_temp_dir="/tmp/grafana-import",
        parallel_table_priority=(),
        verify_tables=(),
        dry_run=False,
        dry_run_parallel=False,
        cleanup_temp=False,
        purge_temp=True,
        ssl_ca=None,
        ssl_cert=None,
        ssl_key=None,
        ssl_disabled=False,
    )


def _line_for_offset(statement: str, start_line: int, offset: int) -> int:
    return start_line + statement[:offset].count("\n")


def _snippet_around(statement: str, start: int, end: int, radius: int = 60) -> str:
    left = max(0, start - radius)
    right = min(len(statement), end + radius)
    return statement[left:right].replace("\n", "\\n")


def _table_allowed(table_filter: tuple[str, ...], table_name: str | None) -> bool:
    if not table_filter:
        return True
    if table_name is None:
        return False
    return table_name.split(".")[-1] in table_filter


def _scan_statement(
    statement: str,
    start_line: int,
    end_line: int,
    opts: ImportOptions,
) -> list[Finding]:
    findings: list[Finding] = []
    table = extract_insert_table(statement)
    transformed = maybe_transform_statement(statement, opts)

    if statement.lstrip().upper().startswith(_INSERT_OR_REPLACE_PREFIX):
        if transformed is None:
            findings.append(
                Finding(
                    severity="medium",
                    code="transform_dropped_statement",
                    table=table,
                    start_line=start_line,
                    end_line=end_line,
                    detail="INSERT OR REPLACE statement was dropped by the transform step.",
                    recommendation="Send me this statement and its surrounding lines; the importer should not discard it.",
                    snippet=_snippet_around(statement, 0, min(len(statement), 120)),
                )
            )
        else:
            original_payload = statement.lstrip()[len(_INSERT_OR_REPLACE_PREFIX):]
            transformed_payload = transformed[len(_REPLACE_PREFIX):]
            if original_payload != transformed_payload:
                findings.append(
                    Finding(
                        severity="high",
                        code="transform_payload_changed",
                        table=table,
                        start_line=start_line,
                        end_line=end_line,
                        detail="The rewrite changed bytes after the leading keyword.",
                        recommendation="Send me this finding; the transform path needs a code fix.",
                        snippet=_snippet_around(statement, 0, min(len(statement), 120)),
                    )
                )

    for match in _BACKSLASH_QUOTE_RE.finditer(statement):
        backslashes, quote = match.groups()
        count = len(backslashes)
        if quote != "'":
            continue
        if count % 2 == 0:
            findings.append(
                Finding(
                    severity="high",
                    code="even_backslashes_before_single_quote",
                    table=table,
                    start_line=_line_for_offset(statement, start_line, match.start()),
                    end_line=end_line,
                    detail=(
                        f"Found {count} consecutive backslashes immediately before a single quote. "
                        "The current splitter only checks one previous character and may keep the quote open."
                    ),
                    recommendation=(
                        "High-risk for dashboards/alerts with JSON payloads. "
                        "Pass me this line range and snippet so I can patch the splitter."
                    ),
                    snippet=_snippet_around(statement, match.start(), match.end()),
                )
            )
        elif count >= 3:
            findings.append(
                Finding(
                    severity="low",
                    code="many_backslashes_before_single_quote",
                    table=table,
                    start_line=_line_for_offset(statement, start_line, match.start()),
                    end_line=end_line,
                    detail=(
                        f"Found {count} consecutive backslashes before a single quote. "
                        "This is not automatically wrong, but it is worth inspecting if the row failed."
                    ),
                    recommendation=(
                        "If this row is missing or quarantined, send me the exact statement and the importer error."
                    ),
                    snippet=_snippet_around(statement, match.start(), match.end()),
                )
            )

    return findings


def scan_dump(dump_file: str, table_filter: tuple[str, ...]) -> list[Finding]:
    opts = _opts()
    findings: list[Finding] = []
    with open(dump_file, "r", encoding="utf-8", errors="replace") as fp:
        for statement, start_line, end_line in statement_splitter(fp):
            table = extract_insert_table(statement)
            if not _table_allowed(table_filter, table):
                continue
            findings.extend(_scan_statement(statement, start_line, end_line, opts))
    return findings


def _render_text(findings: list[Finding], limit: int) -> str:
    shown = findings[:limit]
    lines = [
        f"Findings: {len(findings)} total"
        + (f" (showing first {len(shown)})" if len(findings) > len(shown) else "")
    ]
    for finding in shown:
        lines.append(
            f"[{finding.severity}] {finding.code} lines={finding.start_line}-{finding.end_line} "
            f"table={finding.table or '-'}"
        )
        lines.append(f"  detail: {finding.detail}")
        lines.append(f"  recommendation: {finding.recommendation}")
        lines.append(f"  snippet: {finding.snippet}")
    if not findings:
        lines.append("No high-signal quote/backslash risks found with the current heuristics.")
    return "\n".join(lines)


def main(argv: Iterable[str]) -> int:
    parser = build_parser()
    args = parser.parse_args(list(argv))
    table_filter = tuple(
        table.strip() for table in str(args.tables or "").split(",") if table.strip()
    )
    findings = scan_dump(args.dump_file, table_filter)
    if args.json:
        print(json.dumps([asdict(finding) for finding in findings[: args.limit]], indent=2))
    else:
        print(_render_text(findings, args.limit))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
