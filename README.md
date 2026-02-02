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
log_file =
cleanup_temp = false
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

Environment overrides
---------------------
You can override selected connection settings via environment variables:
- `MYSQL_HOST`, `MYSQL_PORT`, `MYSQL_USER`, `MYSQL_PASSWORD`

If `password` is empty or not provided, the CLI will prompt at runtime.

Progress bar and log file
-------------------------
Use `--progress-bar` for a live single-line progress display. Use `--log-file`
to write logs to a file while also printing to stdout.

Cleanup temp files
------------------
Use `--cleanup-temp` to remove per-table temp files after a successful run.

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
