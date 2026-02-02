#!/usr/bin/env python3
"""Grafana SQL dump importer for MySQL."""

from __future__ import annotations

import logging
import sys

from modules.args import parse_args
from modules.importer import format_summary, import_dump
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
