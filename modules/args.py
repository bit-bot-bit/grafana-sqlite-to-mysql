from __future__ import annotations

import argparse
import configparser
import logging
from getpass import getpass
from typing import Iterable, Optional

from .env import env_override
from .types import ImportOptions, ParseError


def build_arg_parser() -> argparse.ArgumentParser:
    # This code here defines the CLI options and defaults.
    parser = argparse.ArgumentParser(
        description="Stream-import a large Grafana SQL dump into MySQL safely."
    )
    parser.add_argument(
        "--config", default=None, help="INI config file with default settings"
    )
    parser.add_argument("--dump-file", required=False, help="Path to .sql dump file")
    parser.add_argument("--target-db", required=False, help="Target database name")
    parser.add_argument("--host", default="127.0.0.1", help="MySQL host")
    parser.add_argument("--port", type=int, default=3306, help="MySQL port")
    parser.add_argument("--user", default="root", help="MySQL user")
    parser.add_argument("--password", default="", help="MySQL password")
    parser.add_argument(
        "--commit-statements",
        type=int,
        default=500,
        help="Commit every N statements",
    )
    parser.add_argument(
        "--commit-bytes",
        type=int,
        default=5 * 1024 * 1024,
        help="Commit every M bytes",
    )
    parser.add_argument(
        "--disable-foreign-keys",
        action="store_true",
        help="Disable foreign key checks for session",
    )
    parser.add_argument(
        "--disable-unique-checks",
        action="store_true",
        help="Disable unique checks for session",
    )
    parser.add_argument("--sql-mode", default=None, help="Set session sql_mode")
    parser.add_argument(
        "--autocommit",
        action="store_true",
        help="Enable autocommit (disables transactional batching)",
    )
    parser.add_argument(
        "--force-charset",
        default="utf8mb4",
        help="Force connection charset (default utf8mb4)",
    )
    parser.add_argument(
        "--create-db",
        action="store_true",
        help="Create target database if it does not exist",
    )
    parser.add_argument(
        "--recreate-db",
        action="store_true",
        help="Drop and recreate target database",
    )
    parser.add_argument(
        "--quarantine-file",
        default="quarantine_failures.sql",
        help="File to store failing statements",
    )
    parser.add_argument(
        "--quarantine-all-failures",
        action="store_true",
        default=True,
        help="Quarantine any failing statement (default true)",
    )
    parser.add_argument(
        "--quarantine-only-inserts",
        action="store_true",
        help="Quarantine only failing INSERT/REPLACE statements",
    )
    parser.add_argument(
        "--fail-on-error",
        action="store_true",
        help="Exit non-zero if any statement fails",
    )
    parser.add_argument(
        "--progress-mb",
        type=int,
        default=50,
        help="Log progress every X MB",
    )
    parser.add_argument(
        "--progress-statements",
        type=int,
        default=5000,
        help="Log progress every Y statements",
    )
    parser.add_argument(
        "--progress-bar",
        action="store_true",
        help="Show a live progress bar in the terminal",
    )
    parser.add_argument(
        "--progress-bar-logs",
        action="store_true",
        help="Keep periodic progress log lines even with progress bar",
    )
    parser.add_argument(
        "--worker-progress",
        action="store_true",
        help="Show per-worker table progress logs in parallel mode",
    )
    parser.add_argument(
        "--worker-progress-interval",
        type=float,
        default=5.0,
        help="Seconds between worker progress table updates",
    )
    parser.add_argument(
        "--log-file",
        default=None,
        help="Write logs to a file in addition to stdout",
    )
    parser.add_argument(
        "--cleanup-temp",
        action="store_true",
        help="Remove per-table temp files after success",
    )
    parser.add_argument(
        "--ignore-locks",
        action="store_true",
        help="Ignore LOCK TABLES and UNLOCK TABLES",
    )
    parser.add_argument(
        "--allow-delimiter",
        action="store_true",
        help="Allow DELIMITER directives (not supported; only skip line)",
    )
    parser.add_argument(
        "--no-transforms",
        action="store_true",
        help="Disable statement transformations",
    )
    parser.add_argument(
        "--transform-insert-or-replace",
        action="store_true",
        default=True,
        help="Transform INSERT OR REPLACE into REPLACE",
    )
    parser.add_argument(
        "--parallel-per-table",
        action="store_true",
        help="Stage INSERT/REPLACE statements per table and import in parallel",
    )
    parser.add_argument(
        "--parallel-workers",
        type=int,
        default=4,
        help="Number of parallel workers for per-table import",
    )
    parser.add_argument(
        "--parallel-temp-dir",
        default="/tmp/grafana-import",
        help="Temp directory for per-table staging",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse and report without executing statements",
    )
    parser.add_argument(
        "--dry-run-parallel",
        action="store_true",
        help="In dry-run mode, stage per-table temp files for parallel import",
    )
    parser.add_argument("--ssl-ca", default=None, help="SSL CA file")
    parser.add_argument("--ssl-cert", default=None, help="SSL cert file")
    parser.add_argument("--ssl-key", default=None, help="SSL key file")
    parser.add_argument(
        "--ssl-disabled", action="store_true", help="Disable SSL"
    )
    return parser


def _config_bool(value: Optional[str]) -> Optional[bool]:
    if value is None:
        return None
    value = value.strip().lower()
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off"}:
        return False
    return None


def load_config(path: str) -> dict:
    # This code here reads INI config into a dict of defaults.
    parser = configparser.ConfigParser()
    if not parser.read(path):
        raise ParseError(f"Config file not found or unreadable: {path}")

    def get(section: str, key: str) -> Optional[str]:
        if parser.has_option(section, key):
            return parser.get(section, key)
        return None

    config: dict = {}

    for key in ("host", "port", "user", "password", "target_db"):
        value = get("mysql", key) or get("import", key)
        if value is not None:
            config[key] = value

    for key in ("ssl_ca", "ssl_cert", "ssl_key", "ssl_disabled"):
        value = get("mysql", key)
        if value is not None:
            config[key] = value

    for key in (
        "dump_file",
        "commit_statements",
        "commit_bytes",
        "disable_foreign_keys",
        "disable_unique_checks",
        "sql_mode",
        "autocommit",
        "force_charset",
        "create_db",
        "recreate_db",
        "quarantine_file",
        "quarantine_all_failures",
        "quarantine_only_inserts",
        "fail_on_error",
        "progress_mb",
        "progress_statements",
        "progress_bar",
        "progress_bar_logs",
        "worker_progress",
        "worker_progress_interval",
        "log_file",
        "cleanup_temp",
        "ignore_locks",
        "allow_delimiter",
        "no_transforms",
        "transform_insert_or_replace",
        "parallel_per_table",
        "parallel_workers",
        "parallel_temp_dir",
        "dry_run",
        "dry_run_parallel",
    ):
        value = get("import", key)
        if value is not None:
            config[key] = value

    for key in (
        "port",
        "commit_statements",
        "commit_bytes",
        "progress_mb",
        "progress_statements",
        "parallel_workers",
    ):
        if key in config:
            config[key] = int(str(config[key]).strip())

    for key in (
        "disable_foreign_keys",
        "disable_unique_checks",
        "autocommit",
        "create_db",
        "recreate_db",
        "quarantine_all_failures",
        "quarantine_only_inserts",
        "fail_on_error",
        "ignore_locks",
        "allow_delimiter",
        "no_transforms",
        "transform_insert_or_replace",
        "parallel_per_table",
        "dry_run",
        "dry_run_parallel",
        "cleanup_temp",
        "ssl_disabled",
    ):
        if key in config:
            coerced = _config_bool(str(config[key]))
            if coerced is not None:
                config[key] = coerced

    return config


def parse_args(argv: Iterable[str]) -> ImportOptions:
    # This code here merges config + CLI + env vars and prompts for password if needed.
    parser = build_arg_parser()
    argv_list = list(argv)
    prelim, _ = parser.parse_known_args(argv_list)
    if prelim.config:
        config_defaults = load_config(prelim.config)
        parser.set_defaults(**config_defaults)
        logging.info("Loaded config defaults from %s", prelim.config)

    args = parser.parse_args(argv_list)

    if not args.dump_file:
        parser.error("--dump-file is required (or set dump_file in config)")
    if not args.target_db:
        parser.error("--target-db is required (or set target_db in config)")
    if args.parallel_workers < 1:
        parser.error("--parallel-workers must be >= 1")

    if args.quarantine_only_inserts and args.quarantine_all_failures:
        args.quarantine_all_failures = False

    def provided(flag: str) -> bool:
        return flag in argv_list

    host = args.host
    if not provided("--host"):
        host = env_override(host, "MYSQL_HOST") or host

    port = args.port
    if not provided("--port"):
        env_port = env_override(None, "MYSQL_PORT")
        if env_port:
            port = int(env_port)

    user = args.user
    if not provided("--user"):
        user = env_override(user, "MYSQL_USER") or user

    password = args.password
    if not args.dry_run:
        if not provided("--password"):
            password = env_override(password, "MYSQL_PASSWORD") or password
        if password == "":
            password = None
        if not password:
            password = getpass("MySQL password: ")

    return ImportOptions(
        dump_file=args.dump_file,
        host=host,
        port=port,
        user=user,
        password=password,
        target_db=args.target_db,
        commit_statements=args.commit_statements,
        commit_bytes=args.commit_bytes,
        disable_foreign_keys=args.disable_foreign_keys,
        disable_unique_checks=args.disable_unique_checks,
        sql_mode=args.sql_mode,
        autocommit=args.autocommit,
        force_charset=args.force_charset,
        create_db=args.create_db,
        recreate_db=args.recreate_db,
        quarantine_file=args.quarantine_file,
        quarantine_all_failures=args.quarantine_all_failures,
        quarantine_only_inserts=args.quarantine_only_inserts,
        fail_on_error=args.fail_on_error,
        progress_mb=args.progress_mb,
        progress_statements=args.progress_statements,
        progress_bar=args.progress_bar,
        progress_bar_logs=args.progress_bar_logs,
        worker_progress=args.worker_progress,
        worker_progress_interval=args.worker_progress_interval,
        log_file=args.log_file,
        ignore_locks=args.ignore_locks,
        allow_delimiter=args.allow_delimiter,
        no_transforms=args.no_transforms,
        transform_insert_or_replace=args.transform_insert_or_replace,
        parallel_per_table=args.parallel_per_table,
        parallel_workers=args.parallel_workers,
        parallel_temp_dir=args.parallel_temp_dir,
        dry_run=args.dry_run,
        dry_run_parallel=args.dry_run_parallel,
        cleanup_temp=args.cleanup_temp,
        ssl_ca=args.ssl_ca,
        ssl_cert=args.ssl_cert,
        ssl_key=args.ssl_key,
        ssl_disabled=args.ssl_disabled,
    )
