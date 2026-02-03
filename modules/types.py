from __future__ import annotations

import dataclasses
from typing import Optional


@dataclasses.dataclass
class ImportStats:
    # This code here is a small bucket of counters for reporting.
    statements_total: int = 0
    statements_ok: int = 0
    statements_failed: int = 0
    bytes_read: int = 0
    start_time: float = 0.0


@dataclasses.dataclass
class ImportOptions:
    # This code here is the full config blob for the importer.
    dump_file: str
    host: str
    port: int
    user: str
    password: str
    target_db: str
    commit_statements: int
    commit_bytes: int
    disable_foreign_keys: bool
    disable_unique_checks: bool
    sql_mode: Optional[str]
    autocommit: bool
    force_charset: Optional[str]
    create_db: bool
    recreate_db: bool
    quarantine_file: str
    quarantine_all_failures: bool
    quarantine_only_inserts: bool
    fail_on_error: bool
    progress_mb: int
    progress_statements: int
    progress_bar: bool
    log_file: Optional[str]
    progress_bar_logs: bool
    worker_progress: bool
    worker_progress_interval: float
    auto_tune_batch: bool
    resume: bool
    resume_file: str
    ignore_locks: bool
    allow_delimiter: bool
    no_transforms: bool
    transform_insert_or_replace: bool
    parallel_per_table: bool
    parallel_workers: int
    parallel_temp_dir: str
    dry_run: bool
    dry_run_parallel: bool
    cleanup_temp: bool
    purge_temp: bool
    ssl_ca: Optional[str]
    ssl_cert: Optional[str]
    ssl_key: Optional[str]
    ssl_disabled: bool


class ParseError(Exception):
    pass
