from __future__ import annotations

import datetime as dt
import json
import logging
import os
import queue
import sys
import threading
import time
from typing import Iterable, Optional

from .db import (
    apply_session_toggles,
    build_connection,
    ensure_database,
    fetch_server_identity,
    restore_session_toggles,
    select_database,
)
from .parser import (
    extract_insert_table,
    is_insert_or_replace_or_update,
    maybe_transform_statement,
    split_insert_values,
    statement_splitter,
)
from .types import ImportOptions, ImportStats

DEFAULT_PARALLEL_TABLE_PRIORITY = (
    "org",
    "user",
    "team",
    "folder",
    "dashboard",
    "data_source",
)
_STAGE_DIRNAME = "tables"


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


def _merge_insert_statements(
    group: list[tuple[str, int, int]],
) -> tuple[str, int, int] | None:
    if len(group) < 2:
        return None
    prefix = None
    values: list[str] = []
    for statement, _, _ in group:
        parts = split_insert_values(statement)
        if parts is None:
            return None
        stmt_prefix, stmt_values = parts
        if prefix is None:
            prefix = stmt_prefix
        elif stmt_prefix != prefix:
            return None
        values.append(stmt_values.rstrip().rstrip(";"))
    if prefix is None:
        return None
    return prefix + ",".join(values) + ";", group[0][1], group[-1][2]


def _coalesce_batch(
    batch: list[tuple[str, int, int]],
    opts: ImportOptions,
) -> list[tuple[tuple[str, int, int], list[tuple[str, int, int]]]]:
    if not opts.combine_inserts:
        return [
            ((statement, start_line, end_line), [(statement, start_line, end_line)])
            for statement, start_line, end_line in batch
        ]

    groups: list[tuple[tuple[str, int, int], list[tuple[str, int, int]]]] = []
    pending: list[tuple[str, int, int]] = []
    pending_prefix: str | None = None

    def flush_pending() -> None:
        nonlocal pending, pending_prefix
        if not pending:
            return
        merged = _merge_insert_statements(pending)
        if merged is None:
            for item in pending:
                groups.append((item, [item]))
        else:
            groups.append((merged, list(pending)))
        pending = []
        pending_prefix = None

    for statement, start_line, end_line in batch:
        parts = split_insert_values(statement)
        if parts is None:
            flush_pending()
            groups.append(
                ((statement, start_line, end_line), [(statement, start_line, end_line)])
            )
            continue
        stmt_prefix, _ = parts
        if pending and (
            stmt_prefix != pending_prefix
            or len(pending) >= opts.combine_insert_group_size
        ):
            flush_pending()
        pending.append((statement, start_line, end_line))
        pending_prefix = stmt_prefix
    flush_pending()
    return groups


def _ordered_table_items(
    table_files: dict[str, str], table_priority: tuple[str, ...]
) -> list[tuple[str, str]]:
    # This code here lets operators pull selected tables to the front of the queue.
    effective_priority = table_priority or DEFAULT_PARALLEL_TABLE_PRIORITY
    priority_rank = {name: index for index, name in enumerate(effective_priority)}
    return sorted(
        table_files.items(),
        key=lambda item: (
            priority_rank.get(item[0], len(priority_rank)),
            item[0],
        ),
    )


def _parallel_stage_dir(base_dir: str) -> str:
    return os.path.join(base_dir, _STAGE_DIRNAME)


def _parallel_stage_path(base_dir: str, table_name: str) -> str:
    safe_name = table_name.replace("/", "_").replace(".", "__")
    return os.path.join(_parallel_stage_dir(base_dir), f"{safe_name}.sql")


def _purge_parallel_stage_dir(base_dir: str) -> None:
    stage_dir = _parallel_stage_dir(base_dir)
    if not os.path.isdir(stage_dir):
        return
    for name in os.listdir(stage_dir):
        if name.endswith(".sql"):
            try:
                os.remove(os.path.join(stage_dir, name))
            except Exception:
                logging.warning("Failed to remove temp file %s", name)


def _should_process_statement(statement: str, opts: ImportOptions) -> bool:
    table_name = extract_insert_table(statement)
    if not opts.table_filter:
        return True
    if table_name:
        return table_name.split(".")[-1] in opts.table_filter
    return False


def _log_server_identity(conn) -> None:
    identity = fetch_server_identity(conn)
    logging.info(
        "Server: host=%s uuid=%s read_only=%s database=%s",
        identity["hostname"],
        identity["server_uuid"],
        identity["read_only"],
        identity["database"],
    )


class ProgressBar:
    def __init__(self, enabled: bool, total_bytes: Optional[int]) -> None:
        # This code here does a lightweight CLI progress line with ETA.
        self.enabled = enabled
        self.total_bytes = total_bytes
        self.last_update = 0.0
        self.start_time = time.monotonic()
        self.width = 28

    def update(self, stats: ImportStats) -> None:
        if not self.enabled:
            return
        now = time.monotonic()
        if now - self.last_update < 0.2:
            return
        self.last_update = now
        if self.total_bytes and self.total_bytes > 0:
            pct = min(stats.bytes_read / self.total_bytes, 1.0) * 100.0
            filled = int(self.width * (pct / 100.0))
            bar = "█" * filled + "░" * (self.width - filled)
            elapsed = max(time.monotonic() - self.start_time, 0.001)
            bytes_per_sec = stats.bytes_read / elapsed
            eta = (self.total_bytes - stats.bytes_read) / bytes_per_sec if bytes_per_sec > 0 else 0
            stmts_per_sec = stats.statements_total / elapsed
            msg = (
                f"\r[{bar}] {pct:6.2f}% "
                f"{stats.bytes_read/1024/1024:8.2f}MB/"
                f"{self.total_bytes/1024/1024:8.2f}MB "
                f"stmts={stats.statements_total} ok={stats.statements_ok} "
                f"fail={stats.statements_failed} "
                f"{stmts_per_sec:6.1f} stmts/s ETA {eta:6.1f}s"
            )
        else:
            msg = (
                f"\r[{'█' * (self.width // 3)}{'░' * (self.width - self.width // 3)}] "
                f"{stats.bytes_read/1024/1024:8.2f}MB "
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
    grouped_batch = _coalesce_batch(batch, opts)
    try:
        if not opts.autocommit:
            conn.begin()
        for (statement, _, _), _original_group in grouped_batch:
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
        for (statement, start_line, end_line), original_group in grouped_batch:
            try:
                if not opts.autocommit:
                    conn.begin()
                execute_statement(cursor, statement)
                if not opts.autocommit:
                    conn.commit()
                if stats_lock:
                    with stats_lock:
                        stats.statements_ok += len(original_group)
                else:
                    stats.statements_ok += len(original_group)
                ok += len(original_group)
            except Exception as err:
                if not opts.autocommit:
                    conn.rollback()
                if len(original_group) == 1:
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
                    continue

                for original_statement, original_start, original_end in original_group:
                    try:
                        if not opts.autocommit:
                            conn.begin()
                        execute_statement(cursor, original_statement)
                        if not opts.autocommit:
                            conn.commit()
                        if stats_lock:
                            with stats_lock:
                                stats.statements_ok += 1
                        else:
                            stats.statements_ok += 1
                        ok += 1
                    except Exception as item_err:
                        if not opts.autocommit:
                            conn.rollback()
                        if stats_lock:
                            with stats_lock:
                                stats.statements_failed += 1
                        else:
                            stats.statements_failed += 1
                        failed += 1
                        if should_quarantine(original_statement, opts):
                            if quarantine_lock:
                                with quarantine_lock:
                                    write_quarantine(
                                        quarantine_fp,
                                        original_statement,
                                        item_err,
                                        original_start,
                                        original_end,
                                    )
                            else:
                                write_quarantine(
                                    quarantine_fp,
                                    original_statement,
                                    item_err,
                                    original_start,
                                    original_end,
                                )
                        logging.error(
                            "Statement failed at lines %d-%d: %s",
                            original_start,
                            original_end,
                            item_err,
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
    worker_status: dict[int, dict],
    worker_lock: threading.Lock,
    completed_tables: list[tuple[str, int, int, int]],
    completed_lock: threading.Lock,
    resume_path: Optional[str],
    worker_errors: list[tuple[int, str, str]],
    error_lock: threading.Lock,
    stop_event: threading.Event,
) -> None:
    # This code here is a worker that eats one table file at a time.
    conn = build_connection(opts)
    select_database(conn, opts.target_db)
    original_fk = None
    original_uniq = None
    try:
        original_fk, original_uniq = apply_session_toggles(conn, opts)
        while True:
            if stop_event.is_set():
                break
            item = table_queue.get()
            if item is None:
                table_queue.task_done()
                break
            table_name, file_path = item
            logging.info("Worker %d processing table %s", worker_id, table_name)
            commit_statements = opts.commit_statements
            commit_bytes = opts.commit_bytes
            if opts.auto_tune_batch:
                try:
                    size_mb = os.path.getsize(file_path) / (1024 * 1024)
                    if size_mb >= 1024:
                        commit_statements = max(commit_statements, 10000)
                        commit_bytes = max(commit_bytes, 100 * 1024 * 1024)
                    elif size_mb >= 200:
                        commit_statements = max(commit_statements, 5000)
                        commit_bytes = max(commit_bytes, 50 * 1024 * 1024)
                    elif size_mb >= 50:
                        commit_statements = max(commit_statements, 2000)
                        commit_bytes = max(commit_bytes, 20 * 1024 * 1024)
                except Exception:
                    pass
            batch: list[tuple[str, int, int]] = []
            batch_bytes = 0
            table_ok = 0
            table_failed = 0
            with worker_lock:
                worker_status[worker_id] = {
                    "table": table_name,
                    "processed": 0,
                    "total": table_totals.get(table_name, 0),
                    "failed": 0,
                }
            try:
                with open(file_path, "r", encoding="utf-8", errors="replace") as fp:
                    for statement, start_line, end_line in statement_splitter(fp):
                        stmt_text, src_start, src_end = _extract_source_lines(
                            statement, start_line, end_line
                        )
                        batch.append((stmt_text, src_start, src_end))
                        batch_bytes += len(statement.encode("utf-8"))
                        if len(batch) >= commit_statements or batch_bytes >= commit_bytes:
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
                            with worker_lock:
                                status = worker_status.get(worker_id, {})
                                status["processed"] = table_ok + table_failed
                                status["failed"] = table_failed
                                worker_status[worker_id] = status
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
                        with worker_lock:
                            status = worker_status.get(worker_id, {})
                            status["processed"] = table_ok + table_failed
                            status["failed"] = table_failed
                            worker_status[worker_id] = status
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
                with completed_lock:
                    completed_tables.append((table_name, processed, total, table_failed))
                    if resume_path:
                        _write_resume_file(
                            resume_path,
                            {
                                "mode": "parallel",
                                "completed_tables": [t[0] for t in completed_tables],
                            },
                        )
            finally:
                with worker_lock:
                    worker_status[worker_id] = {
                        "table": None,
                        "processed": 0,
                        "total": 0,
                        "failed": 0,
                    }
                table_queue.task_done()
    except Exception as err:
        stop_event.set()
        with error_lock:
            worker_errors.append((worker_id, worker_status.get(worker_id, {}).get("table") or "-", str(err)))
        logging.error("Worker %d failed: %s", worker_id, err)
        while True:
            try:
                pending = table_queue.get_nowait()
            except queue.Empty:
                break
            table_queue.task_done()
            if pending is None:
                continue
    finally:
        if original_fk is not None or original_uniq is not None:
            try:
                restore_session_toggles(conn, original_fk, original_uniq)
            except Exception:
                logging.warning("Worker failed to restore session toggles")
        conn.close()


def _render_worker_table(
    worker_status: dict[int, dict],
    completed_tables: list[tuple[str, int, int, int]],
    total_tables: int,
) -> str:
    lines = ["Worker Progress:"]
    for wid in sorted(worker_status.keys()):
        st = worker_status.get(wid, {})
        table = st.get("table") or "-"
        total = st.get("total") or 0
        processed = st.get("processed") or 0
        failed = st.get("failed") or 0
        pct = (processed / total * 100.0) if total else 0.0
        lines.append(
            f"  #{wid} table={table} {pct:6.2f}% ({processed}/{total}) failures={failed}"
        )
    completed_count = len(completed_tables)
    lines.append(f"Completed Tables: {completed_count}/{total_tables}")
    for name, processed, total, failed in completed_tables[-5:]:
        lines.append(f"  {name} 100.00% ({processed}/{total}) failures={failed}")
    return "\n".join(lines) + "\n"


def _write_resume_file(path: str, data: dict) -> None:
    # This code here writes a small checkpoint so we can resume safely.
    tmp_path = f"{path}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as fp:
        json.dump(data, fp)
    os.replace(tmp_path, path)


def _read_resume_file(path: str) -> Optional[dict]:
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as fp:
            return json.load(fp)
    except Exception:
        return None


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
    worker_lock = threading.Lock()
    completed_lock = threading.Lock()
    worker_status: dict[int, dict] = {}
    completed_tables: list[tuple[str, int, int, int]] = []
    completed_names: set[str] = set()
    worker_errors: list[tuple[int, str, str]] = []
    error_lock = threading.Lock()
    stop_event = threading.Event()

    os.makedirs(opts.parallel_temp_dir, exist_ok=True)
    os.makedirs(_parallel_stage_dir(opts.parallel_temp_dir), exist_ok=True)
    resume_path = opts.resume_file if opts.resume else None
    if resume_path:
        resume_data = _read_resume_file(resume_path)
        if resume_data and resume_data.get("mode") == "parallel":
            completed_names = set(resume_data.get("completed_tables", []))
    if opts.purge_temp:
        _purge_parallel_stage_dir(opts.parallel_temp_dir)

    progress = ProgressBar(opts.progress_bar, os.path.getsize(opts.dump_file))
    try:
        ensure_database(conn, opts)
        _log_server_identity(conn)
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
                if not opts.progress_bar or opts.progress_bar_logs:
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
                if not _should_process_statement(transformed, opts):
                    continue

                table_name = extract_insert_table(transformed)
                if table_name:
                    path = table_files.get(table_name)
                    if not path:
                        path = _parallel_stage_path(opts.parallel_temp_dir, table_name)
                        table_files[table_name] = path
                        table_fps[table_name] = open(
                            path, "w", encoding="utf-8"
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
            batch = []

        for fp_out in table_fps.values():
            fp_out.close()
        if table_counts:
            logging.info("Parallel staging summary: %d tables", len(table_counts))
            for name, count in sorted(table_counts.items(), key=lambda x: x[1], reverse=True)[:10]:
                size_mb = table_bytes.get(name, 0) / (1024 * 1024)
                logging.info("  table=%s statements=%d size=%.2fMB", name, count, size_mb)

        if completed_names:
            with completed_lock:
                for name in completed_names:
                    total = table_counts.get(name, 0)
                    completed_tables.append((name, total, total, 0))

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
                    worker_status,
                    worker_lock,
                    completed_tables,
                    completed_lock,
                    resume_path,
                    worker_errors,
                    error_lock,
                    stop_event,
                ),
                daemon=True,
            )
            thread.start()
            workers.append(thread)

        if opts.worker_progress:
            logging.info("Workers: %d (progress updates every %.1fs)", opts.parallel_workers, opts.worker_progress_interval)
            with worker_lock:
                for i in range(1, opts.parallel_workers + 1):
                    worker_status.setdefault(i, {"table": None, "processed": 0, "total": 0, "failed": 0})

        def progress_table():
            while True:
                time.sleep(opts.worker_progress_interval)
                if not opts.worker_progress:
                    continue
                if table_queue.unfinished_tasks == 0:
                    break
                with worker_lock:
                    with completed_lock:
                        table_text = _render_worker_table(
                            worker_status,
                            list(completed_tables),
                            total_tables=len(table_files),
                        )
                sys.stderr.write("\033[H\033[J")
                sys.stderr.write(table_text)
                sys.stderr.flush()

        progress_thread = None
        if opts.worker_progress:
            progress_thread = threading.Thread(target=progress_table, daemon=True)
            progress_thread.start()

        for table_name, path in _ordered_table_items(
            table_files, opts.parallel_table_priority
        ):
            if table_name in completed_names:
                logging.info("Skipping completed table %s (resume)", table_name)
                continue
            table_queue.put((table_name, path))
        for _ in workers:
            table_queue.put(None)

        table_queue.join()
        if progress_thread:
            progress_thread.join()
        for thread in workers:
            thread.join()
        if worker_errors:
            worker_id, table_name, err = worker_errors[0]
            raise RuntimeError(
                f"Worker {worker_id} failed while processing table {table_name}: {err}"
            )

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
                os.rmdir(_parallel_stage_dir(opts.parallel_temp_dir))
            except Exception:
                pass
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
            os.makedirs(_parallel_stage_dir(opts.parallel_temp_dir), exist_ok=True)
            if opts.purge_temp:
                _purge_parallel_stage_dir(opts.parallel_temp_dir)
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
                if not _should_process_statement(transformed, opts):
                    continue
                stats.statements_ok += 1
                if opts.dry_run_parallel:
                    table_name = extract_insert_table(transformed)
                    if table_name:
                        path = table_files.get(table_name)
                        if not path:
                            path = _parallel_stage_path(opts.parallel_temp_dir, table_name)
                            table_files[table_name] = path
                            table_fps[table_name] = open(path, "w", encoding="utf-8")
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
                if not opts.progress_bar or opts.progress_bar_logs:
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
                os.rmdir(_parallel_stage_dir(opts.parallel_temp_dir))
            except Exception:
                pass
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

    commit_statements = opts.commit_statements
    commit_bytes = opts.commit_bytes
    if opts.auto_tune_batch:
        try:
            size_mb = os.path.getsize(opts.dump_file) / (1024 * 1024)
            if size_mb >= 1024:
                commit_statements = max(commit_statements, 10000)
                commit_bytes = max(commit_bytes, 100 * 1024 * 1024)
            elif size_mb >= 200:
                commit_statements = max(commit_statements, 5000)
                commit_bytes = max(commit_bytes, 50 * 1024 * 1024)
            elif size_mb >= 50:
                commit_statements = max(commit_statements, 2000)
                commit_bytes = max(commit_bytes, 20 * 1024 * 1024)
        except Exception:
            pass

    progress = ProgressBar(opts.progress_bar, os.path.getsize(opts.dump_file))
    resume_data = None
    if opts.resume:
        resume_data = _read_resume_file(opts.resume_file)
        if resume_data and resume_data.get("mode") == "linear":
            offset = int(resume_data.get("offset", 0))
            if offset > 0:
                logging.info("Resuming from byte offset %d", offset)
    file_offset = 0
    last_stmt_offset = 0
    try:
        ensure_database(conn, opts)
        _log_server_identity(conn)
        original_fk, original_uniq = apply_session_toggles(conn, opts)

        quarantine_fp = open(opts.quarantine_file, "a", encoding="utf-8")
        batch_bytes = 0
        last_mb_reported = -1
        last_stmt_reported = 0

        with open(opts.dump_file, "r", encoding="utf-8", errors="replace") as fp:
            if opts.resume and resume_data and resume_data.get("mode") == "linear":
                offset = int(resume_data.get("offset", 0))
                if offset > 0:
                    fp.seek(offset)

            file_offset = fp.tell()

            def line_reader() -> Iterable[str]:
                nonlocal file_offset, last_stmt_offset
                for line in fp:
                    line_bytes = len(line.encode("utf-8"))
                    stats.bytes_read += line_bytes
                    file_offset += line_bytes
                    last_stmt_offset = file_offset
                    progress.update(stats)
                    yield line

            for statement, start_line, end_line in statement_splitter(line_reader()):
                stats.statements_total += 1
                if not opts.progress_bar or opts.progress_bar_logs:
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
                if not _should_process_statement(transformed, opts):
                    continue

                batch.append((transformed, start_line, end_line))
                batch_bytes += len(transformed.encode("utf-8"))

                if opts.autocommit or (
                    len(batch) >= commit_statements
                    or batch_bytes >= commit_bytes
                ):
                    process_batch(conn, batch, stats, opts, quarantine_fp)
                    if opts.resume:
                        _write_resume_file(
                            opts.resume_file,
                            {"mode": "linear", "offset": last_stmt_offset},
                        )
                    batch = []
                    batch_bytes = 0

        if batch:
            process_batch(conn, batch, stats, opts, quarantine_fp)
            if opts.resume:
                _write_resume_file(
                    opts.resume_file,
                    {"mode": "linear", "offset": last_stmt_offset},
                )
            batch = []

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
