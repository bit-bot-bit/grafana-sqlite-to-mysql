#!/usr/bin/env python3
"""Grafana SQL dump importer for MySQL."""

from __future__ import annotations

import logging
import sys

from modules.args import parse_args
from modules.confirm import prompt_for_config_confirmation
from modules.importer import (
    DEFAULT_PARALLEL_TABLE_PRIORITY,
    _parallel_stage_dir,
    format_summary,
    import_dump,
)
from modules.types import ParseError


def setup_logging() -> None:
    # This code here sets up default stdout logging.
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.StreamHandler()],
    )


def add_log_file(log_file: str | None) -> None:
    # This code here adds an optional log file handler.
    if not log_file:
        return
    handler = logging.FileHandler(log_file)
    handler.setLevel(logging.INFO)
    handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logging.getLogger().addHandler(handler)


def main(argv: list[str]) -> int:
    # This code here is the CLI entrypoint.
    setup_logging()
    try:
        opts = parse_args(argv)
        add_log_file(opts.log_file)
        if opts.config_path and not opts.skip_config_confirm:
            ssl_mode = "disabled" if opts.ssl_disabled else (
                "custom" if (opts.ssl_ca or opts.ssl_cert or opts.ssl_key) else "default"
            )
            ssl_ca_display = (
                "<ignored because ssl_disabled=true>"
                if opts.ssl_disabled and opts.ssl_ca
                else opts.ssl_ca
            )
            ssl_cert_display = (
                "<ignored because ssl_disabled=true>"
                if opts.ssl_disabled and opts.ssl_cert
                else opts.ssl_cert
            )
            ssl_key_display = (
                "<ignored because ssl_disabled=true>"
                if opts.ssl_disabled and opts.ssl_key
                else opts.ssl_key
            )
            prompt_for_config_confirmation(
                "Resolved import settings:",
                opts.config_path,
                (
                    ("dump_file", opts.dump_file),
                    (
                        "tables",
                        ",".join(opts.table_filter) or "<all>",
                    ),
                    ("target_db", opts.target_db),
                    ("host", opts.host),
                    ("port", opts.port),
                    ("user", opts.user),
                    ("password", "<hidden>" if opts.password else "<prompted>"),
                    ("ssl_mode", ssl_mode),
                    ("ssl_ca", ssl_ca_display),
                    ("ssl_cert", ssl_cert_display),
                    ("ssl_key", ssl_key_display),
                    ("commit_statements", opts.commit_statements),
                    ("commit_bytes", opts.commit_bytes),
                    ("autocommit", opts.autocommit),
                    ("combine_inserts", opts.combine_inserts),
                    ("combine_insert_group_size", opts.combine_insert_group_size),
                    ("create_db", opts.create_db),
                    ("recreate_db", opts.recreate_db),
                    ("parallel_per_table", opts.parallel_per_table),
                    ("parallel_workers", opts.parallel_workers),
                    ("ordered_table_insert", opts.ordered_table_insert),
                    (
                        "parallel_stage_dir",
                        _parallel_stage_dir(opts.parallel_temp_dir),
                    ),
                    (
                        "parallel_table_priority",
                        ",".join(opts.parallel_table_priority)
                        or ",".join(DEFAULT_PARALLEL_TABLE_PRIORITY),
                    ),
                    ("dry_run", opts.dry_run),
                    ("resume", opts.resume),
                    ("quarantine_file", opts.quarantine_file),
                ),
            )
        logging.info(
            "Mode: %s",
            "DRY RUN (no DB connection)" if opts.dry_run else "LIVE IMPORT",
        )
        logging.info(
            "Settings: dump=%s target_db=%s host=%s port=%s user=%s ssl=%s",
            opts.dump_file,
            opts.target_db,
            opts.host,
            opts.port,
            opts.user,
            "disabled" if opts.ssl_disabled else ("on" if opts.ssl_ca else "default"),
        )
        logging.info(
            "Settings: commit_statements=%d commit_bytes=%d autocommit=%s parallel=%s workers=%d ordered_table_insert=%s dry_run_parallel=%s",
            opts.commit_statements,
            opts.commit_bytes,
            opts.autocommit,
            opts.parallel_per_table,
            opts.parallel_workers,
            opts.ordered_table_insert,
            opts.dry_run_parallel,
        )
        stats = import_dump(opts)
        logging.info(format_summary(stats, opts))
        if opts.fail_on_error and stats.statements_failed > 0:
            return 2
        return 0
    except ParseError as err:
        logging.error("Parsing failed: %s", err)
        return 3
    except Exception as err:
        logging.error("Fatal error: %s", err)
        return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
