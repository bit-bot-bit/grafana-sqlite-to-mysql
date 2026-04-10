#!/usr/bin/env python3
"""Verify MySQL table row counts against a SQL dump."""

from __future__ import annotations

import argparse
import logging
import sys
from collections import defaultdict
from getpass import getpass
from typing import Iterable

from modules.db import detect_driver
from modules.parser import count_insert_values_rows, extract_insert_table, statement_splitter


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Compare expected row counts from a SQL dump with actual MySQL table counts."
    )
    parser.add_argument("--dump-file", required=True, help="Path to .sql dump file")
    parser.add_argument("--target-db", required=True, help="Target database name")
    parser.add_argument("--host", default="127.0.0.1", help="MySQL host")
    parser.add_argument("--port", type=int, default=3306, help="MySQL port")
    parser.add_argument("--user", default="root", help="MySQL user")
    parser.add_argument("--password", default="", help="MySQL password")
    parser.add_argument("--tables", default="", help="Comma-separated table names to verify; defaults to all tables seen in the dump")
    parser.add_argument("--ssl-ca", default=None, help="SSL CA file")
    parser.add_argument("--ssl-cert", default=None, help="SSL cert file")
    parser.add_argument("--ssl-key", default=None, help="SSL key file")
    parser.add_argument("--ssl-disabled", action="store_true", help="Disable SSL")
    return parser


def build_connection(args: argparse.Namespace):
    driver = detect_driver(prefer_mysqlclient=True)
    ssl = None
    if not args.ssl_disabled and (args.ssl_ca or args.ssl_cert or args.ssl_key):
        ssl = {}
        if args.ssl_ca:
            ssl["ca"] = args.ssl_ca
        if args.ssl_cert:
            ssl["cert"] = args.ssl_cert
        if args.ssl_key:
            ssl["key"] = args.ssl_key

    if driver == "mysqlclient":
        import MySQLdb  # type: ignore
        from MySQLdb.constants import CLIENT as MYSQLCLIENT  # type: ignore

        kwargs = {
            "host": args.host,
            "port": args.port,
            "user": args.user,
            "passwd": args.password,
            "charset": "utf8mb4",
            "use_unicode": True,
            "autocommit": True,
            "client_flag": MYSQLCLIENT.MULTI_STATEMENTS,
        }
        if ssl is not None:
            kwargs["ssl"] = ssl
        return MySQLdb.connect(**kwargs)

    if driver == "pymysql":
        import pymysql  # type: ignore
        from pymysql.constants import CLIENT as PYMYSQLCLIENT  # type: ignore

        kwargs = {
            "host": args.host,
            "port": args.port,
            "user": args.user,
            "password": args.password,
            "charset": "utf8mb4",
            "autocommit": True,
            "client_flag": PYMYSQLCLIENT.MULTI_STATEMENTS,
        }
        if ssl is not None:
            kwargs["ssl"] = ssl
        return pymysql.connect(**kwargs)

    raise RuntimeError("No compatible driver.")


def expected_row_counts(dump_file: str, table_filter: tuple[str, ...]) -> dict[str, int]:
    counts: dict[str, int] = defaultdict(int)
    with open(dump_file, "r", encoding="utf-8", errors="replace") as fp:
        for statement, _start_line, _end_line in statement_splitter(fp):
            table_name = extract_insert_table(statement)
            if not table_name:
                continue
            short_name = table_name.split(".")[-1]
            if table_filter and short_name not in table_filter:
                continue
            row_count = count_insert_values_rows(statement)
            if row_count is None:
                counts[short_name] += 1
            else:
                counts[short_name] += row_count
    return dict(counts)


def actual_row_count(conn, db_name: str, table_name: str) -> tuple[bool, int]:
    cursor = conn.cursor()
    try:
        cursor.execute(f"USE `{db_name}`")
        cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`")
        return True, int(cursor.fetchone()[0])
    except Exception:
        return False, 0
    finally:
        cursor.close()


def verify_tables(args: argparse.Namespace) -> int:
    table_filter = tuple(
        table.strip() for table in str(args.tables or "").split(",") if table.strip()
    )
    expected = expected_row_counts(args.dump_file, table_filter)
    if not expected:
        logging.error("No insert-backed tables found in %s", args.dump_file)
        return 1

    conn = build_connection(args)
    mismatches = 0
    try:
        logging.info("Verifying %d tables against %s", len(expected), args.target_db)
        for table_name in sorted(expected):
            exists, actual = actual_row_count(conn, args.target_db, table_name)
            status = "OK"
            if not exists:
                status = "MISSING"
                mismatches += 1
            elif actual != expected[table_name]:
                status = "MISMATCH"
                mismatches += 1
            logging.info(
                "table=%s expected=%d actual=%s status=%s",
                table_name,
                expected[table_name],
                actual if exists else "<missing>",
                status,
            )
    finally:
        conn.close()

    if mismatches:
        logging.error("Verification failed: %d table(s) mismatched or missing", mismatches)
        return 2
    logging.info("Verification passed: all %d table(s) matched", len(expected))
    return 0


def main(argv: Iterable[str]) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    parser = build_parser()
    args = parser.parse_args(list(argv))
    if args.password == "":
        args.password = getpass("MySQL password: ")
    return verify_tables(args)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
