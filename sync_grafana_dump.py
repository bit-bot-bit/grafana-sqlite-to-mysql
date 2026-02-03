#!/usr/bin/env python3
"""Grafana SQL dump sync tool (diff + upsert by natural keys)."""

from __future__ import annotations

import argparse
import configparser
import logging
import sys
from typing import Iterable
from getpass import getpass

from modules.args import _config_bool
from modules.sync import DEFAULT_TABLES, sync_from_dump
from modules.types import ImportOptions, ParseError


def build_parser() -> argparse.ArgumentParser:
    # This code here defines the sync CLI flags.
    parser = argparse.ArgumentParser(
        description="Diff and upsert Grafana data from a SQL dump using natural keys."
    )
    parser.add_argument("--config", default=None, help="INI config file")
    parser.add_argument("--dump-file", required=False, help="Path to .sql dump file")
    parser.add_argument("--target-db", required=False, help="Target database name")
    parser.add_argument("--host", default="127.0.0.1", help="MySQL host")
    parser.add_argument("--port", type=int, default=3306, help="MySQL port")
    parser.add_argument("--user", default="root", help="MySQL user")
    parser.add_argument("--password", default="", help="MySQL password")
    parser.add_argument("--ssl-ca", default=None, help="SSL CA file")
    parser.add_argument("--ssl-cert", default=None, help="SSL cert file")
    parser.add_argument("--ssl-key", default=None, help="SSL key file")
    parser.add_argument("--ssl-disabled", action="store_true", help="Disable SSL")
    parser.add_argument(
        "--stage-db",
        default="__grafana_sync_stage",
        help="Staging database for diff/apply",
    )
    parser.add_argument(
        "--tables",
        default=",".join(DEFAULT_TABLES),
        help="Comma-separated list of tables to sync",
    )
    parser.add_argument(
        "--diff-only",
        action="store_true",
        help="Only compute diff (default true unless --apply is set)",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply upserts to target",
    )
    return parser


def load_config(path: str) -> dict:
    # This code here reads sync settings from INI.
    parser = configparser.ConfigParser()
    if not parser.read(path):
        raise ParseError(f"Config file not found or unreadable: {path}")

    def get(section: str, key: str):
        if parser.has_option(section, key):
            return parser.get(section, key)
        return None

    config: dict = {}
    for key in ("host", "port", "user", "password", "target_db"):
        value = get("mysql", key) or get("sync", key)
        if value is not None:
            config[key] = value

    for key in ("ssl_ca", "ssl_cert", "ssl_key", "ssl_disabled"):
        value = get("mysql", key)
        if value is not None:
            config[key] = value

    for key in ("dump_file", "stage_db", "tables", "diff_only", "apply"):
        value = get("sync", key)
        if value is not None:
            config[key] = value

    if "port" in config:
        config["port"] = int(str(config["port"]).strip())

    for key in ("ssl_disabled", "diff_only", "apply"):
        if key in config:
            coerced = _config_bool(str(config[key]))
            if coerced is not None:
                config[key] = coerced

    return config


def parse_args(argv: Iterable[str]) -> argparse.Namespace:
    # This code here merges config + CLI and validates required fields.
    parser = build_parser()
    prelim, _ = parser.parse_known_args(argv)
    if prelim.config:
        config_defaults = load_config(prelim.config)
        parser.set_defaults(**config_defaults)

    args = parser.parse_args(argv)
    if not args.dump_file:
        parser.error("--dump-file is required (or set dump_file in config)")
    if not args.target_db:
        parser.error("--target-db is required (or set target_db in config)")
    return args


def to_import_options(args: argparse.Namespace) -> ImportOptions:
    # This code here builds the shared ImportOptions from sync args.
    password = args.password
    if password == "":
        password = None
    if not password:
        password = getpass("MySQL password: ")
    return ImportOptions(
        dump_file=args.dump_file,
        host=args.host,
        port=args.port,
        user=args.user,
        password=password,
        target_db=args.target_db,
        commit_statements=500,
        commit_bytes=5 * 1024 * 1024,
        disable_foreign_keys=False,
        disable_unique_checks=False,
        sql_mode=None,
        autocommit=False,
        force_charset="utf8mb4",
        create_db=False,
        recreate_db=False,
        quarantine_file="quarantine_failures.sql",
        quarantine_all_failures=True,
        quarantine_only_inserts=False,
        fail_on_error=False,
        progress_mb=50,
        progress_statements=5000,
        progress_bar=False,
        progress_bar_logs=False,
        worker_progress=False,
        worker_progress_interval=5.0,
        log_file=None,
        cleanup_temp=False,
        purge_temp=True,
        ignore_locks=False,
        allow_delimiter=False,
        no_transforms=False,
        transform_insert_or_replace=True,
        parallel_per_table=False,
        parallel_workers=4,
        parallel_temp_dir="/tmp/grafana-import",
        dry_run=False,
        dry_run_parallel=False,
        ssl_ca=args.ssl_ca,
        ssl_cert=args.ssl_cert,
        ssl_key=args.ssl_key,
        ssl_disabled=args.ssl_disabled,
    )


def main(argv: Iterable[str]) -> int:
    # This code here is the sync entrypoint.
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    try:
        args = parse_args(argv)
        if args.stage_db == args.target_db:
            raise ParseError("stage_db must be different from target_db")
        opts = to_import_options(args)
        tables = [t.strip() for t in args.tables.split(",") if t.strip()]
        diff_only = args.diff_only or not args.apply
        logging.info("Sync tables: %s", ", ".join(tables))
        results = sync_from_dump(
            opts,
            dump_file=args.dump_file,
            target_db=args.target_db,
            stage_db=args.stage_db,
            tables=tables,
            diff_only=diff_only,
            apply=args.apply,
        )
        for r in results:
            logging.info(
                "Diff %s: stage=%d new=%d changed=%d unchanged=%d",
                r["table"],
                r["stage_total"],
                r["new"],
                r["changed"],
                r["unchanged"],
            )
        return 0
    except ParseError as err:
        logging.error("Parsing failed: %s", err)
        return 3
    except Exception as err:
        logging.error("Fatal error: %s", err)
        return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
