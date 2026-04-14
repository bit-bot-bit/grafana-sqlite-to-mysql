Grafana SQL Dump Importer (MySQL)
=================================

Overview
--------
This CLI streams a large Grafana SQL dump into a MySQL database without loading the
entire dump into memory. It batches statements into transactions, quarantines
failures for later review, and continues importing unless instructed to fail.

Prerequisites and workflow
--------------------------
1. Extract `grafana.db` from your Grafana instance.
2. Run Grafana's database migrator to produce the SQL dump, following:
   `https://github.com/grafana/database-migrator`
3. Start Grafana pointed at the new MySQL target so it can hydrate the base
   schema (create tables, indexes, and initial metadata).
4. Stop Grafana.
5. Run this importer to apply the INSERT/REPLACE data from the dump.
6. Start Grafana again against the MySQL target.

Requirements
------------
- Python 3.11+
- MySQL driver: mysqlclient (preferred) or PyMySQL

Example usage
-------------
Simple import:
```
python import_grafana_dump.py --dump-file grafana.sql --target-db grafana \
  --host 127.0.0.1 --user root --password secret
```

Diff + upsert sync (natural keys):
```
python sync_grafana_dump.py --dump-file grafana.sql --target-db grafana \
  --tables dashboard,folder,data_source,alert_rule --diff-only
```

Apply sync (upsert):
```
python sync_grafana_dump.py --dump-file grafana.sql --target-db grafana \
  --tables dashboard,folder,data_source,alert_rule --apply
```

Import with performance flags:
```
python import_grafana_dump.py --dump-file grafana.sql --target-db grafana \
  --disable-foreign-keys --disable-unique-checks --sql-mode "" \
  --commit-statements 1000 --commit-bytes $((10*1024*1024))
```

Import with recreate-db and quarantine file:
```
python import_grafana_dump.py --dump-file grafana.sql --target-db grafana \
  --recreate-db --quarantine-file bad_inserts.sql --fail-on-error
```

Config file (INI)
-----------------
Use a single config file for Azure or other managed MySQL:
```
python import_grafana_dump.py --config grafana_import.ini
```

Example grafana_import.ini (see `grafana_import.ini.example`):
```
[mysql]
host = 127.0.0.1
port = 3306
user = root
password =
target_db = grafana
ssl_ca = /path/to/ca.pem
ssl_cert = /path/to/cert.pem
ssl_key = /path/to/key.pem
ssl_disabled = false

[import]
dump_file = /path/to/grafana.sql
commit_statements = 500
commit_bytes = 5242880
disable_foreign_keys = true
disable_unique_checks = true
sql_mode =
autocommit = false
combine_inserts = false
combine_insert_group_size = 25
force_charset = utf8mb4
create_db = true
recreate_db = false
quarantine_file = quarantine_failures.sql
quarantine_all_failures = true
quarantine_only_inserts = false
fail_on_error = false
progress_mb = 50
progress_statements = 5000
progress_bar = false
progress_bar_logs = false
worker_progress = false
worker_progress_interval = 5.0
no_auto_tune_batch = false
resume = false
resume_file = import.resume.json
log_file =
cleanup_temp = false
no_purge_temp = false
ignore_locks = true
allow_delimiter = false
no_transforms = false
transform_insert_or_replace = true
parallel_per_table = false
parallel_workers = 4
parallel_temp_dir = /tmp/grafana-import
dry_run = false
dry_run_parallel = false
```

Example sync section (optional):
```
[sync]
dump_file = /path/to/grafana.sql
target_db = grafana
stage_db = __grafana_sync_stage
tables = dashboard,dashboard_version,dashboard_acl,folder,data_source,alert_rule
diff_only = true
apply = false
```

Environment overrides
---------------------
You can override selected connection settings via environment variables:
- `MYSQL_HOST`, `MYSQL_PORT`, `MYSQL_USER`, `MYSQL_PASSWORD`

If `password` is empty or not provided, the CLI will prompt at runtime.

Progress bar and log file
-------------------------
Use `--progress-bar` for a live single-line progress display. Use `--log-file`
to write logs to a file while also printing to stdout.
Use `--progress-bar-logs` to keep periodic progress log lines even when the
progress bar is enabled.

Auto-tuned batch sizes
----------------------
By default, large dumps (or large per-table files in parallel mode) will auto-tune
`commit_statements` and `commit_bytes` upward. Use `--no-auto-tune-batch` to disable.

Combined multi-row inserts
--------------------------
Use `--combine-inserts` to merge consecutive compatible `INSERT` or `REPLACE`
statements into a single multi-row statement before execution. Use
`--combine-insert-group-size` to cap how many original statements may be merged
at once.

If a merged statement fails, the importer falls back to the original statements
inside that merged group and continues isolating failures normally.

This option is most useful when the source dump is dominated by one-row or
very small insert statements. If the dump already uses multi-row inserts, extra
coalescing may be neutral or slower and should be benchmarked before making it
the default.

Resume mode
-----------
Use `--resume` to write and reuse a checkpoint file (`--resume-file`) so the import
can continue after interruption. In parallel mode, completed tables are skipped on resume.

Worker progress table (parallel mode)
-------------------------------------
Use `--worker-progress` to print a periodic table of worker progress while
running `--parallel-per-table`. Use `--worker-progress-interval` to control the
refresh rate in seconds.
This view redraws in-place using ANSI escape codes.

Cleanup temp files
------------------
Use `--cleanup-temp` to remove per-table temp files after a successful run.
By default, temp files are purged before staging. Use `--no-purge-temp` to keep them.

Parallel per-table mode
-----------------------
Enable with `--parallel-per-table` to stage INSERT/REPLACE statements per table
into temp files, then import those files concurrently. Non-INSERT statements
are executed serially in the main pass. This is best when tables are independent
and foreign keys are disabled. Temp files are written under `parallel_temp_dir`.

Dry run
-------
Use `--dry-run` to parse and report stats without executing SQL. This skips
password prompting and does not connect to MySQL.

Use `--dry-run-parallel` with `--dry-run` to stage per-table temp files for
parallel import sizing.

Performance simulation
----------------------
Use `generate_perf_fixture.py` to build a synthetic benchmark fixture with many
tables and inserts:
```
python3 generate_perf_fixture.py --output-dir .perf-fixture --target-size-mib 1024
```

Then use `docker-compose.perf.yml` with Docker or Podman to load `schema.sql`
and run the importer against the generated `dump.sql`. See `PERF_SIMULATION.md`
for the full workflow.

Post-import verification
------------------------
Use `verify_import_tables.py` to compare expected row counts from a dump against
actual row counts in MySQL:

```bash
python3 verify_import_tables.py \
  --dump-file grafana.sql \
  --target-db grafana \
  --host 127.0.0.1 \
  --user root
```

Pre-scan dump for risky data rows
--------------------------------
Use `scan_sql_dump_risks.py` to scan a dump for data patterns that are likely to
confuse the importer, especially rows with backslash-plus-quote sequences in
dashboard or alert payloads.

Scan the full dump:
```bash
python3 scan_sql_dump_risks.py --dump-file grafana.sql
```

Scan only dashboards and alerts:
```bash
python3 scan_sql_dump_risks.py \
  --dump-file grafana.sql \
  --tables dashboard,alert_rule \
  --limit 100
```

Emit JSON output:
```bash
python3 scan_sql_dump_risks.py \
  --dump-file grafana.sql \
  --tables dashboard,alert_rule \
  --json
```

What it checks:
- High-risk backslash-before-single-quote patterns in statement payloads
- Other multi-backslash quote patterns worth manual review
- Whether the `INSERT OR REPLACE` rewrite changed payload bytes after the keyword swap

What to send back if it finds something:
- The finding code, especially `even_backslashes_before_single_quote`
- The `lines=...` range
- The `table=...` value
- The emitted snippet

This scanner is aimed at bad or risky data rows in the dump itself. It is not a
general SQL validator and does not replace a real import or post-import verification.

To limit verification to selected tables:

```bash
python3 verify_import_tables.py \
  --dump-file grafana.sql \
  --target-db grafana \
  --tables dashboard,annotation,alert_rule_version
```

This script compares row counts implied by the dump's `INSERT` or `REPLACE`
statements with live MySQL counts. If the import quarantined failures or skipped
bad rows, mismatches are expected and should be interpreted together with the
quarantine file.

Benchmark notes
---------------
Recent local benchmark artifacts in this repo:

- `reports/TEST_REPORT.md`: current unit-test status
- `PERFORMANCE_TUNING.md`: configuration guidance for throughput tuning
- `reports/PERF_SMOKE_REPORT.md`: end-to-end synthetic smoke test
- `reports/PERF_150MB_REPORT.md`: 150 MiB comparison of baseline vs `--combine-inserts`
- `reports/ITERATION_TEST_REPORT.md`: bad-insert fallback behavior for merged groups

The 150 MiB benchmark showed:

- baseline parallel import: `25.5s`
- combined inserts (`--combine-inserts --combine-insert-group-size 25`): `33.1s`

That synthetic dump already used 10 rows per source `INSERT`, so the benchmark
shows that additional coalescing is not automatically a win. The feature is
more likely to help when the source dump uses one-row inserts or very small
insert statements.

Behavior notes
--------------
- Streams and splits SQL statements by semicolons not inside quotes/backticks.
- Quarantines failing statements with timestamps, line ranges, and a snippet.
- If a batch fails, rolls back and retries statements one-by-one to isolate errors.
- Supports optional transforms to ignore SQLite pragmas and convert INSERT OR REPLACE.
- Can ignore LOCK TABLES/UNLOCK TABLES when requested.

Exit codes
----------
- 0: completed (even with quarantined failures)
- 2: failures and --fail-on-error set
- 3: parsing error (e.g., DELIMITER without --allow-delimiter)
- 1: fatal errors (connection/IO)
