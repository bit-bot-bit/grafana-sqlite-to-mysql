# 150 MiB Import Benchmark

Date: 2026-04-09

## Scope

Measured importer behavior on a synthetic `150 MiB` dump after adding the new optional combined-insert mode.

This benchmark compared:

- baseline parallel import
- parallel import with `--combine-inserts --combine-insert-group-size 25`

## Validation before benchmark

Command:

```bash
python3 -m unittest tests.test_importer tests.test_parser tests.test_perf_fixture
```

Result:

- `20` tests passed
- `0` failures
- `0` errors

## Workload

Fixture generation command:

```bash
python3 generate_perf_fixture.py \
  --output-dir .perf-150m \
  --target-size-mib 150 \
  --rows-per-insert 10
```

Fixture manifest summary:

- actual size: `157,307,094` bytes
- generated statements: `4,747`
- rows per insert in source dump: `10`

Key row counts in the generated dump:

- `org`: `1,440`
- `user_account`: `2,880`
- `dashboard`: `4,320`
- `annotation`: `7,160`
- `alert_rule_version`: `5,720`
- `dashboard_version`: `7,200`

Important context:

- This synthetic dump already uses multi-row inserts with `10` rows per `INSERT`.
- That matters because the new feature is most likely to help when the input dump is mostly one-row inserts or very small inserts.

## Environment

- local `podman compose` perf stack from [docker-compose.perf.yml](/home/tearle/Work/sql_dump/git-grafana/grafana-sqlite-to-mysql/docker-compose.perf.yml)
- MySQL 8 container
- verifier container from [Dockerfile.perf-verifier](/home/tearle/Work/sql_dump/git-grafana/grafana-sqlite-to-mysql/Dockerfile.perf-verifier)
- importer settings for both runs:
  - `--disable-foreign-keys`
  - `--disable-unique-checks`
  - `--parallel-per-table`
  - `--parallel-workers 4`

Schema load command:

```bash
podman compose -f docker-compose.perf.yml exec -T mysql \
  mysql -uroot -prootpass -D grafana_perf < .perf-150m/schema.sql
```

## Run 1: Baseline

Command:

```bash
podman compose -f docker-compose.perf.yml exec verifier bash -lc "
  cd /workspace && python3 import_grafana_dump.py \
    --dump-file .perf-150m/dump.sql \
    --target-db grafana_perf \
    --host mysql \
    --port 3306 \
    --user root \
    --password rootpass \
    --disable-foreign-keys \
    --disable-unique-checks \
    --parallel-per-table \
    --parallel-workers 4 \
    --verify-tables org,user_account,dashboard,annotation,alert_rule_version \
    --log-file perf-baseline.log \
    --yes"
```

Observed result:

- completed import: `total=4749 ok=4749 failed=0`
- runtime: `25.5s`

## Run 2: Combined inserts

Command:

```bash
podman compose -f docker-compose.perf.yml exec verifier bash -lc "
  cd /workspace && python3 import_grafana_dump.py \
    --dump-file .perf-150m/dump.sql \
    --target-db grafana_perf \
    --host mysql \
    --port 3306 \
    --user root \
    --password rootpass \
    --disable-foreign-keys \
    --disable-unique-checks \
    --parallel-per-table \
    --parallel-workers 4 \
    --combine-inserts \
    --combine-insert-group-size 25 \
    --verify-tables org,user_account,dashboard,annotation,alert_rule_version \
    --log-file perf-combine.log \
    --yes"
```

Observed result:

- completed import: `total=4749 ok=4749 failed=0`
- runtime: `33.1s`

## Row-count verification

Post-run verification against MySQL:

- `org`: `1,440`
- `user_account`: `2,880`
- `dashboard`: `4,320`
- `annotation`: `7,160`
- `alert_rule_version`: `5,720`

These matched the generated fixture manifest.

## Comparison

- baseline: `25.5s`
- combined inserts: `33.1s`
- delta: `+7.6s`
- relative change: about `29.8% slower`

## Interpretation

On this workload, combined inserts were slower, not faster.

The main reason is likely the shape of the source dump:

- the generated dump already used `10` rows per source `INSERT`
- enabling `--combine-inserts --combine-insert-group-size 25` let the importer merge up to `25` of those source statements
- that means some executed statements became roughly `250` rows each

That is a much larger executed statement, and on this local MySQL benchmark it appears to have increased statement parsing and execution cost more than it reduced round trips.

## Important limitation of this result

This benchmark does **not** mean combined inserts are broadly a bad idea.

It means:

- for a dump that already contains reasonably chunky multi-row inserts
- on this local containerized MySQL setup
- with parallel per-table import already enabled

the extra coalescing was not beneficial.

The feature is still more likely to help when:

- the source dump is mostly one-row inserts
- network latency to MySQL is significant
- the original dump has many tiny insert statements

## Fallback-path note

Neither benchmark run triggered statement failures, so the new fallback behavior for failed merged groups was not exercised in the speed test itself.

That fallback behavior is covered by unit tests in [tests/test_importer.py](/home/tearle/Work/sql_dump/git-grafana/grafana-sqlite-to-mysql/tests/test_importer.py).

## Recommendation

Use `--combine-inserts` selectively.

Good candidates:

- row-at-a-time dumps
- tiny insert statements
- higher-latency remote MySQL targets

For dumps that already use multi-row inserts, start conservatively:

- try `--combine-insert-group-size 5` or `10` first
- benchmark before adopting it as a default
