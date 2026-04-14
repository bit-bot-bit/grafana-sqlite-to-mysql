# Performance Tuning Guide

This guide explains how to tune [import_grafana_dump.py](/home/tearle/Work/sql_dump/git-grafana/grafana-sqlite-to-mysql/import_grafana_dump.py) for large Grafana SQL imports.

It is based on:

- importer behavior in this repo
- local synthetic benchmarks
- live validation of fallback and verification paths

## First principles

Import speed is mostly controlled by four things:

1. how many SQL statements must cross the connection
2. how often transactions are committed
3. whether tables can be loaded in parallel
4. whether MySQL is local or remote

That last point matters a lot. A setting that is neutral or slower on local MySQL can still help over a higher-latency network connection.

## Recommended starting point

Use this as a baseline for large imports:

```ini
[import]
commit_statements = 500
commit_bytes = 5242880
disable_foreign_keys = true
disable_unique_checks = true
sql_mode =
autocommit = false
parallel_per_table = true
parallel_workers = 4
parallel_temp_dir = /tmp/grafana-import
combine_inserts = false
combine_insert_group_size = 10
progress_statements = 5000
progress_bar = true
worker_progress = true
create_db = true
recreate_db = false
dry_run = false
dry_run_parallel = false
```

And run with:

```bash
python3 import_grafana_dump.py --config grafana_import.ini --yes
```

If you are importing only selected tables:

```bash
python3 import_grafana_dump.py \
  --config grafana_import.ini \
  --tables annotation,annotation_tag \
  --verify-tables annotation,annotation_tag \
  --yes
```

## High-impact settings

### `parallel_per_table`

Use `parallel_per_table = true` when:

- tables are largely independent
- foreign keys are disabled during import
- you have large tables that can be loaded concurrently

This is usually the biggest speed lever in this importer.

Use `parallel_workers` to control concurrency:

- `4` is a good starting point
- try `6` or `8` only if the MySQL server and network can sustain it
- more workers are not always better

Too many workers can increase contention and reduce throughput.

### `commit_statements` and `commit_bytes`

These define the outer transaction batch.

Good starting values:

- `commit_statements = 500`
- `commit_bytes = 5242880` (5 MiB)

Larger values can help if:

- latency is high
- MySQL handles larger transactions well
- the dump has many small statements

But going too large can increase:

- rollback cost
- memory pressure
- per-batch failure impact

This importer also auto-tunes these upward for larger workloads unless you disable that with `no_auto_tune_batch = true`.

### `disable_foreign_keys` and `disable_unique_checks`

For bulk import, these are usually worth enabling:

```ini
disable_foreign_keys = true
disable_unique_checks = true
```

Benefits:

- less validation overhead during load
- more flexibility in table ordering

Tradeoff:

- MySQL does not retroactively validate old rows when you turn checks back on
- if the dump is inconsistent, bad references can remain

Use these when you trust the dump and care about speed.

### `autocommit`

Keep this `false` for bulk import.

```ini
autocommit = false
```

That allows batching and explicit commits. `autocommit = true` usually increases overhead for large loads.

## Combined inserts

### What it does

`combine_inserts = true` tells the importer to merge consecutive compatible `INSERT` or `REPLACE` statements into larger multi-row statements before execution.

Example:

```sql
INSERT INTO t VALUES (1);
INSERT INTO t VALUES (2);
INSERT INTO t VALUES (3);
```

can become:

```sql
INSERT INTO t VALUES (1),(2),(3);
```

### Fallback behavior

If a merged statement fails:

- the importer does **not** abandon the whole batch
- it falls back to the original statements inside that merged group
- good rows can still succeed
- bad rows can still be quarantined

This fallback was validated in [ITERATION_TEST_REPORT.md](/home/tearle/Work/sql_dump/git-grafana/grafana-sqlite-to-mysql/reports/ITERATION_TEST_REPORT.md).

### When it is most likely to help

Use `combine_inserts = true` when:

- the dump is mostly one-row inserts
- or very small inserts
- and MySQL is remote over a higher-latency network

That is the case where reducing round trips matters most.

### When it may not help

It may be neutral or slower when:

- the dump already uses multi-row inserts
- statements are already large
- MySQL is local or low-latency
- parallel loading is already doing most of the work

This was exactly what happened in the local 150 MiB benchmark:

- baseline parallel import: `25.5s`
- `combine_insert_group_size = 25`: `33.1s`
- `combine_insert_group_size = 10`: `33.1s`

That benchmark used a synthetic dump that already had 10 rows per source insert. Extra coalescing made statements larger without enough network savings to justify it.

### Recommended tuning for `combine_insert_group_size`

Start conservatively:

- `5`
- `10`

Avoid assuming larger is better.

For remote MySQL, try:

```ini
combine_inserts = true
combine_insert_group_size = 10
```

Then compare runtime against:

```ini
combine_inserts = false
```

If your dump is already multi-row, leave it off unless benchmarking proves otherwise.

## Local vs remote guidance

### Local MySQL

Likely winners:

- `parallel_per_table = true`
- sensible `parallel_workers`
- disabled FK and unique checks
- tuned batch sizes

Likely neutral or risky:

- aggressive insert coalescing on already large inserts

### Remote MySQL

Likely winners:

- `parallel_per_table = true`
- larger but still sane batching
- moderate `combine_inserts` if the dump uses tiny inserts

Remote targets are where statement-count reduction matters more.

## Safe workflow

1. Hydrate schema first with Grafana.
2. Stop Grafana.
3. Run the importer.
4. Verify counts.
5. Start Grafana again.

Do not leave Grafana writing to the same DB during the import.

## Verification

Use table verification after important runs:

```bash
python3 import_grafana_dump.py \
  --config grafana_import.ini \
  --verify-tables org,user_account,dashboard,annotation \
  --yes
```

Or use the standalone verifier:

```bash
python3 verify_import_tables.py \
  --dump-file grafana.sql \
  --target-db grafana \
  --tables dashboard,annotation,alert_rule_version
```

Important:

- `verify_import_tables.py` compares expected dump row counts to actual DB row counts
- if bad rows were quarantined, mismatches are expected and useful

## Practical tuning order

If you need to improve performance, change settings in this order:

1. Enable `parallel_per_table`
2. Tune `parallel_workers`
3. Keep FK and unique checks disabled during import
4. Tune `commit_statements` and `commit_bytes`
5. Only then experiment with `combine_inserts`

That order gives the best chance of gaining speed without overcomplicating failure handling.

## Suggested profiles

### Conservative remote profile

```ini
[import]
commit_statements = 500
commit_bytes = 5242880
disable_foreign_keys = true
disable_unique_checks = true
autocommit = false
parallel_per_table = true
parallel_workers = 4
combine_inserts = true
combine_insert_group_size = 5
```

### Aggressive remote profile

```ini
[import]
commit_statements = 1000
commit_bytes = 10485760
disable_foreign_keys = true
disable_unique_checks = true
autocommit = false
parallel_per_table = true
parallel_workers = 6
combine_inserts = true
combine_insert_group_size = 10
```

### Local or already multi-row dump profile

```ini
[import]
commit_statements = 500
commit_bytes = 5242880
disable_foreign_keys = true
disable_unique_checks = true
autocommit = false
parallel_per_table = true
parallel_workers = 4
combine_inserts = false
```

## Related docs

- [README.md](/home/tearle/Work/sql_dump/git-grafana/grafana-sqlite-to-mysql/README.md)
- [PERF_SIMULATION.md](/home/tearle/Work/sql_dump/git-grafana/grafana-sqlite-to-mysql/PERF_SIMULATION.md)
- [PERF_150MB_REPORT.md](/home/tearle/Work/sql_dump/git-grafana/grafana-sqlite-to-mysql/reports/PERF_150MB_REPORT.md)
- [ITERATION_TEST_REPORT.md](/home/tearle/Work/sql_dump/git-grafana/grafana-sqlite-to-mysql/reports/ITERATION_TEST_REPORT.md)
