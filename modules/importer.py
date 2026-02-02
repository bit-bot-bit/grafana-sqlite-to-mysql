from __future__ import annotations

import datetime as dt
import logging
import os
import queue
import sys
import threading
import time
from typing import Iterable, Optional

from .db import apply_session_toggles, build_connection, ensure_database, restore_session_toggles
from .parser import (
    extract_insert_table,
    is_insert_or_replace_or_update,
    maybe_transform_statement,
    statement_splitter,
)
from .types import ImportOptions, ImportStats


def log_progress(stats: ImportStats) -> None:
    # This code here prints periodic progress logs (separate from the progress bar).
    mb = stats.bytes_read / (1024 * 1024)
    elapsed = time.time() - stats.start_time
    logging.info(
        "Progress: %.2f MB read, %d statements (ok=%d, failed=%d) in %.1fs",
        mb,
        stats.statements_total,
        stats.statements_ok,
        stats.statements_failed,
        elapsed,
    )


def execute_statement(cursor, statement: str) -> None:
    cursor.execute(statement)


class ProgressBar:
    def __init__(self, enabled: bool, total_bytes: Optional[int]) -> None:
        # This code here does a lightweight CLI progress line with ETA.
        self.enabled = enabled
        self.total_bytes = total_bytes
        self.last_update = 0.0
        self.start_time = time.monotonic()

    def update(self, stats: ImportStats) -> None:
        if not self.enabled:
            return
        now = time.monotonic()
        if now - self.last_update < 0.2:
            return
        self.last_update = now
        if self.total_bytes and self.total_bytes > 0:
            pct = min(stats.bytes_read / self.total_bytes, 1.0) * 100.0
            elapsed = max(time.monotonic() - self.start_time, 0.001)
            bytes_per_sec = stats.bytes_read / elapsed
            eta = (self.total_bytes - stats.bytes_read) / bytes_per_sec if bytes_per_sec > 0 else 0
            stmts_per_sec = stats.statements_total / elapsed
            msg = (
                f"\r{pct:6.2f}% "
                f"{stats.bytes_read/1024/1024:8.2f}MB/"
                f"{self.total_bytes/1024/1024:8.2f}MB "
                f"stmts={stats.statements_total} ok={stats.statements_ok} "
                f"fail={stats.statements_failed} "
                f"{stmts_per_sec:6.1f} stmts/s ETA {eta:6.1f}s"
            )
        else:
            msg = (
                f"\r{stats.bytes_read/1024/1024:8.2f}MB "
                f"stmts={stats.statements_total} ok={stats.statements_ok} fail={stats.statements_failed}"
            )
        sys.stderr.write(msg)
        sys.stderr.flush()

    def finish(self) -> None:
        if self.enabled:
            sys.stderr.write("\n")
            sys.stderr.flush()


def _extract_source_lines(statement: str, start_line: int, end_line: int) -> tuple[str, int, int]:
    # This code here pulls original line ranges from staged temp files.
    if statement.startswith("-- source-lines:"):
        first_line, _, rest = statement.partition("\n")
        parts = first_line.replace("-- source-lines:", "").strip().split("-", 1)
        if len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit():
            return rest.lstrip(), int(parts[0]), int(parts[1])
    return statement, start_line, end_line


def should_quarantine(statement: str, opts: ImportOptions) -> bool:
    # This code here decides if a failed statement goes to quarantine.
    if opts.quarantine_only_inserts:
        return is_insert_or_replace_or_update(statement)
    return opts.quarantine_all_failures


def write_quarantine(
    fp,
    statement: str,
    err: Exception,
    start_line: int,
    end_line: int,
) -> None:
    # This code here writes the failure record plus the exact SQL for replay.
    ts = dt.datetime.utcnow().isoformat() + "Z"
    msg = str(err)
    snippet = statement.replace("\n", " ")[:200]
    header = (
        f"-- quarantined at {ts}; lines {start_line}-{end_line}; "
        f"error: {msg}; snippet: {snippet}\n"
    )
    fp.write(header)
    fp.write(statement)
    if not statement.endswith(";"):
        fp.write(";")
    fp.write("\n\n")
    fp.flush()


def process_batch(
    conn,
    batch: list[tuple[str, int, int]],
    stats: ImportStats,
    opts: ImportOptions,
    quarantine_fp,
    stats_lock: Optional[threading.Lock] = None,
    quarantine_lock: Optional[threading.Lock] = None,
) -> tuple[int, int]:
    # This code here tries a batch, then falls back to per-statement to isolate failures.
    cursor = conn.cursor()
    ok = 0
    failed = 0
    try:
        if not opts.autocommit:
            conn.begin()
        for statement, _, _ in batch:
            execute_statement(cursor, statement)
        if not opts.autocommit:
            conn.commit()
        if stats_lock:
            with stats_lock:
                stats.statements_ok += len(batch)
        else:
            stats.statements_ok += len(batch)
        ok += len(batch)
    except Exception:
        if not opts.autocommit:
            conn.rollback()
        for statement, start_line, end_line in batch:
            try:
                if not opts.autocommit:
                    conn.begin()
                execute_statement(cursor, statement)
                if not opts.autocommit:
                    conn.commit()
                if stats_lock:
                    with stats_lock:
                        stats.statements_ok += 1
                else:
                    stats.statements_ok += 1
                ok += 1
            except Exception as err:
                if not opts.autocommit:
                    conn.rollback()
                if stats_lock:
                    with stats_lock:
                        stats.statements_failed += 1
                else:
                    stats.statements_failed += 1
                failed += 1
                if should_quarantine(statement, opts):
                    if quarantine_lock:
                        with quarantine_lock:
                            write_quarantine(
                                quarantine_fp, statement, err, start_line, end_line
                            )
                    else:
                        write_quarantine(
                            quarantine_fp, statement, err, start_line, end_line
                        )
                logging.error(
                    "Statement failed at lines %d-%d: %s",
                    start_line,
                    end_line,
                    err,
                )
                if opts.fail_on_error:
                    raise
    finally:
        cursor.close()
    return ok, failed


def _parallel_worker(
    worker_id: int,
    table_queue: queue.Queue,
    opts: ImportOptions,
    stats: ImportStats,
    stats_lock: threading.Lock,
    quarantine_fp,
    quarantine_lock: threading.Lock,
    table_totals: dict[str, int],
) -> None:
    # This code here is a worker that eats one table file at a time.
    conn = build_connection(opts)
    try:
        while True:
            item = table_queue.get()
            if item is None:
                table_queue.task_done()
                break
            table_name, file_path = item
            logging.info("Worker %d processing table %s", worker_id, table_name)
            batch: list[tuple[str, int, int]] = []
            batch_bytes = 0
            table_ok = 0
            table_failed = 0
            try:
                with open(file_path, "r", encoding="utf-8", errors="replace") as fp:
                    for statement, start_line, end_line in statement_splitter(fp):
                        stmt_text, src_start, src_end = _extract_source_lines(
                            statement, start_line, end_line
                        )
                        batch.append((stmt_text, src_start, src_end))
                        batch_bytes += len(statement.encode("utf-8"))
                        if len(batch) >= opts.commit_statements or batch_bytes >= opts.commit_bytes:
                            ok, failed = process_batch(
                                conn,
                                batch,
                                stats,
                                opts,
                                quarantine_fp,
                                stats_lock=stats_lock,
                                quarantine_lock=quarantine_lock,
                            )
                            table_ok += ok
                            table_failed += failed
                            batch = []
                            batch_bytes = 0
                    if batch:
                        ok, failed = process_batch(
                            conn,
                            batch,
                            stats,
                            opts,
                            quarantine_fp,
                            stats_lock=stats_lock,
                            quarantine_lock=quarantine_lock,
                        )
                        table_ok += ok
                        table_failed += failed
                total = table_totals.get(table_name, table_ok + table_failed)
                processed = table_ok + table_failed
                pct = (processed / total * 100.0) if total else 100.0
                logging.info(
                    "Table %s complete: %.2f%% (%d/%d) failures=%d",
                    table_name,
                    pct,
                    processed,
                    total,
                    table_failed,
                )
            finally:
                table_queue.task_done()
    finally:
        conn.close()


def import_dump_parallel_per_table(opts: ImportOptions) -> ImportStats:
    # This code here stages INSERTs per table, then runs them in parallel.
    stats = ImportStats(start_time=time.time())
    conn = build_connection(opts)
    quarantine_fp = None
    original_fk = None
    original_uniq = None
    batch: list[tuple[str, int, int]] = []
    batch_bytes = 0
    last_mb_reported = -1
    last_stmt_reported = 0
    table_files: dict[str, str] = {}
    table_fps: dict[str, object] = {}
    table_counts: dict[str, int] = {}
    table_bytes: dict[str, int] = {}
    stats_lock = threading.Lock()
    quarantine_lock = threading.Lock()

    os.makedirs(opts.parallel_temp_dir, exist_ok=True)

    progress = ProgressBar(opts.progress_bar, os.path.getsize(opts.dump_file))
    try:
        ensure_database(conn, opts)
        original_fk, original_uniq = apply_session_toggles(conn, opts)
        quarantine_fp = open(opts.quarantine_file, "a", encoding="utf-8")

        with open(opts.dump_file, "r", encoding="utf-8", errors="replace") as fp:
            def line_reader() -> Iterable[str]:
                for line in fp:
                    stats.bytes_read += len(line.encode("utf-8"))
                    progress.update(stats)
                    yield line

            for statement, start_line, end_line in statement_splitter(line_reader()):
                stats.statements_total += 1
                if (
                    opts.progress_statements > 0
                    and stats.statements_total - last_stmt_reported
                    >= opts.progress_statements
                ):
                    last_stmt_reported = stats.statements_total
                    log_progress(stats)
                if opts.progress_mb > 0:
                    mb_read = int(stats.bytes_read / (1024 * 1024))
                    if mb_read != last_mb_reported and mb_read % opts.progress_mb == 0:
                        last_mb_reported = mb_read
                        log_progress(stats)

                transformed = maybe_transform_statement(statement, opts)
                if transformed is None:
                    continue

                table_name = extract_insert_table(transformed)
                if table_name:
                    path = table_files.get(table_name)
                    if not path:
                        safe_name = table_name.replace("/", "_")
                        path = os.path.join(opts.parallel_temp_dir, f"{safe_name}.sql")
                        table_files[table_name] = path
                        table_fps[table_name] = open(
                            path, "a", encoding="utf-8"
                        )
                    fp_out = table_fps[table_name]
                    table_counts[table_name] = table_counts.get(table_name, 0) + 1
                    table_bytes[table_name] = table_bytes.get(table_name, 0) + len(
                        transformed.encode("utf-8")
                    )
                    fp_out.write(f"-- source-lines: {start_line}-{end_line}\n")
                    fp_out.write(transformed)
                    if not transformed.endswith(";"):
                        fp_out.write(";")
                    fp_out.write("\n")
                else:
                    batch.append((transformed, start_line, end_line))
                    batch_bytes += len(transformed.encode("utf-8"))
                    if len(batch) >= opts.commit_statements or batch_bytes >= opts.commit_bytes:
                        process_batch(conn, batch, stats, opts, quarantine_fp)
                        batch = []
                        batch_bytes = 0

        if batch:
            process_batch(conn, batch, stats, opts, quarantine_fp)

        for fp_out in table_fps.values():
            fp_out.close()
        if table_counts:
            logging.info("Parallel staging summary: %d tables", len(table_counts))
            for name, count in sorted(table_counts.items(), key=lambda x: x[1], reverse=True)[:10]:
                size_mb = table_bytes.get(name, 0) / (1024 * 1024)
                logging.info("  table=%s statements=%d size=%.2fMB", name, count, size_mb)

        table_queue: queue.Queue = queue.Queue()
        workers: list[threading.Thread] = []
        for i in range(opts.parallel_workers):
            thread = threading.Thread(
                target=_parallel_worker,
                args=(
                    i + 1,
                    table_queue,
                    opts,
                    stats,
                    stats_lock,
                    quarantine_fp,
                    quarantine_lock,
                    table_counts,
                ),
                daemon=True,
            )
            thread.start()
            workers.append(thread)

        for table_name, path in table_files.items():
            table_queue.put((table_name, path))
        for _ in workers:
            table_queue.put(None)

        table_queue.join()
        for thread in workers:
            thread.join()

        restore_session_toggles(conn, original_fk, original_uniq)

    finally:
        progress.finish()
        if batch:
            try:
                process_batch(conn, batch, stats, opts, quarantine_fp)
            except Exception:
                if opts.fail_on_error:
                    raise
        for fp_out in table_fps.values():
            try:
                fp_out.close()
            except Exception:
                pass
        if opts.cleanup_temp and table_files:
            for path in table_files.values():
                try:
                    os.remove(path)
                except Exception:
                    logging.warning("Failed to remove temp file %s", path)
            try:
                os.rmdir(opts.parallel_temp_dir)
            except Exception:
                pass
        if original_fk is not None or original_uniq is not None:
            try:
                restore_session_toggles(conn, original_fk, original_uniq)
            except Exception:
                logging.warning("Failed to restore session toggles")
        if quarantine_fp is not None:
            quarantine_fp.close()
        conn.close()

    return stats


def import_dump(opts: ImportOptions) -> ImportStats:
    # This code here is the main entry for import (dry run, parallel, or normal).
    stats = ImportStats(start_time=time.time())
    if opts.dry_run:
        progress = ProgressBar(opts.progress_bar, os.path.getsize(opts.dump_file))
        if opts.dry_run_parallel:
            os.makedirs(opts.parallel_temp_dir, exist_ok=True)
            table_files: dict[str, str] = {}
            table_fps: dict[str, object] = {}
            table_counts: dict[str, int] = {}
            table_bytes: dict[str, int] = {}
        else:
            table_files = {}
            table_fps = {}

        with open(opts.dump_file, "r", encoding="utf-8", errors="replace") as fp:
            def line_reader() -> Iterable[str]:
                for line in fp:
                    stats.bytes_read += len(line.encode("utf-8"))
                    progress.update(stats)
                    yield line

            last_mb_reported = -1
            last_stmt_reported = 0
            for statement, start_line, end_line in statement_splitter(line_reader()):
                stats.statements_total += 1
                transformed = maybe_transform_statement(statement, opts)
                if transformed is None:
                    continue
                stats.statements_ok += 1
                if opts.dry_run_parallel:
                    table_name = extract_insert_table(transformed)
                    if table_name:
                        path = table_files.get(table_name)
                        if not path:
                            safe_name = table_name.replace("/", "_")
                            path = os.path.join(opts.parallel_temp_dir, f"{safe_name}.sql")
                            table_files[table_name] = path
                            table_fps[table_name] = open(path, "a", encoding="utf-8")
                        fp_out = table_fps[table_name]
                        table_counts[table_name] = table_counts.get(table_name, 0) + 1
                        table_bytes[table_name] = table_bytes.get(table_name, 0) + len(
                            transformed.encode("utf-8")
                        )
                        fp_out.write(f"-- source-lines: {start_line}-{end_line}\n")
                        fp_out.write(transformed)
                        if not transformed.endswith(";"):
                            fp_out.write(";")
                        fp_out.write("\n")
                if (
                    opts.progress_statements > 0
                    and stats.statements_total - last_stmt_reported
                    >= opts.progress_statements
                ):
                    last_stmt_reported = stats.statements_total
                    log_progress(stats)
                if opts.progress_mb > 0:
                    mb_read = int(stats.bytes_read / (1024 * 1024))
                    if mb_read != last_mb_reported and mb_read % opts.progress_mb == 0:
                        last_mb_reported = mb_read
                        log_progress(stats)
        for fp_out in table_fps.values():
            fp_out.close()
        if opts.dry_run_parallel and table_counts:
            logging.info("Dry-run parallel staging summary: %d tables", len(table_counts))
            for name, count in sorted(table_counts.items(), key=lambda x: x[1], reverse=True)[:10]:
                size_mb = table_bytes.get(name, 0) / (1024 * 1024)
                logging.info("  table=%s statements=%d size=%.2fMB", name, count, size_mb)
        if opts.cleanup_temp and table_files:
            for path in table_files.values():
                try:
                    os.remove(path)
                except Exception:
                    logging.warning("Failed to remove temp file %s", path)
            try:
                os.rmdir(opts.parallel_temp_dir)
            except Exception:
                pass
        progress.finish()
        return stats
    if opts.parallel_per_table:
        return import_dump_parallel_per_table(opts)
    conn = build_connection(opts)
    quarantine_fp = None
    original_fk = None
    original_uniq = None
    batch: list[tuple[str, int, int]] = []

    progress = ProgressBar(opts.progress_bar, os.path.getsize(opts.dump_file))
    try:
        ensure_database(conn, opts)
        original_fk, original_uniq = apply_session_toggles(conn, opts)

        quarantine_fp = open(opts.quarantine_file, "a", encoding="utf-8")
        batch_bytes = 0
        last_mb_reported = -1
        last_stmt_reported = 0

        with open(opts.dump_file, "r", encoding="utf-8", errors="replace") as fp:
            def line_reader() -> Iterable[str]:
                for line in fp:
                    stats.bytes_read += len(line.encode("utf-8"))
                    progress.update(stats)
                    yield line

            for statement, start_line, end_line in statement_splitter(line_reader()):
                stats.statements_total += 1
                if (
                    opts.progress_statements > 0
                    and stats.statements_total - last_stmt_reported
                    >= opts.progress_statements
                ):
                    last_stmt_reported = stats.statements_total
                    log_progress(stats)
                if opts.progress_mb > 0:
                    mb_read = int(stats.bytes_read / (1024 * 1024))
                    if mb_read != last_mb_reported and mb_read % opts.progress_mb == 0:
                        last_mb_reported = mb_read
                        log_progress(stats)

                transformed = maybe_transform_statement(statement, opts)
                if transformed is None:
                    continue

                batch.append((transformed, start_line, end_line))
                batch_bytes += len(transformed.encode("utf-8"))

                if opts.autocommit or (
                    len(batch) >= opts.commit_statements
                    or batch_bytes >= opts.commit_bytes
                ):
                    process_batch(conn, batch, stats, opts, quarantine_fp)
                    batch = []
                    batch_bytes = 0

        if batch:
            process_batch(conn, batch, stats, opts, quarantine_fp)

        restore_session_toggles(conn, original_fk, original_uniq)

    finally:
        progress.finish()
        if batch:
            try:
                process_batch(conn, batch, stats, opts, quarantine_fp)
            except Exception:
                if opts.fail_on_error:
                    raise
        if original_fk is not None or original_uniq is not None:
            try:
                restore_session_toggles(conn, original_fk, original_uniq)
            except Exception:
                logging.warning("Failed to restore session toggles")
        if quarantine_fp is not None:
            quarantine_fp.close()
        conn.close()

    return stats


def format_summary(stats: ImportStats, opts: ImportOptions) -> str:
    elapsed = time.time() - stats.start_time
    return (
        "Completed import: "
        f"total={stats.statements_total} "
        f"ok={stats.statements_ok} "
        f"failed={stats.statements_failed} "
        f"bytes={stats.bytes_read} "
        f"runtime={elapsed:.1f}s "
        f"quarantine={os.path.abspath(opts.quarantine_file)}"
    )
