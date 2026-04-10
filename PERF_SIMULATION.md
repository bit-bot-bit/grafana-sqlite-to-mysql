# Performance Simulation

This project now includes a synthetic workload generator so you can exercise the importer with a large MySQL dataset before pointing it at a real Grafana dump.

## What it generates

`generate_perf_fixture.py` writes:

- `schema.sql`: a synthetic schema with many Grafana-like tables
- `dump.sql`: a large import dump with `TRUNCATE` plus many `INSERT` statements
- `manifest.json`: actual size, statement count, and row counts per table

The default target is roughly `1 GiB`.

## Generate a 1 GiB fixture

```bash
python3 generate_perf_fixture.py --output-dir .perf-fixture --target-size-mib 1024
```

If you want more or fewer rows per statement:

```bash
python3 generate_perf_fixture.py \
  --output-dir .perf-fixture \
  --target-size-mib 1024 \
  --rows-per-insert 10
```

## Start the local MySQL benchmark stack

Use `docker compose` or `podman compose`:

```bash
podman compose -f docker-compose.perf.yml up -d
```

The verifier image in `docker-compose.perf.yml` already includes `PyMySQL` and
`cryptography`, so the importer can connect to MySQL 8 without any manual
package installation.

## Load the synthetic schema

```bash
podman compose -f docker-compose.perf.yml exec -T mysql \
  mysql -uroot -prootpass -D grafana_perf < .perf-fixture/schema.sql
```

## Run the importer against the synthetic dump

```bash
podman compose -f docker-compose.perf.yml exec verifier bash -lc "
  cd /workspace && \
  python3 import_grafana_dump.py \
    --dump-file .perf-fixture/dump.sql \
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
    --yes
"
```

## Inspect the generated workload

```bash
cat .perf-fixture/manifest.json
```

Useful checks:

```bash
podman compose -f docker-compose.perf.yml exec mysql \
  mysql -uroot -prootpass -D grafana_perf -e "
    SELECT COUNT(*) AS dashboards FROM dashboard;
    SELECT COUNT(*) AS annotations FROM annotation;
    SELECT COUNT(*) AS alert_rule_versions FROM alert_rule_version;
  "
```

## Notes

- The synthetic schema uses indexes but avoids enforced foreign keys so the benchmark focuses on importer throughput instead of FK rejection behavior.
- The fixture is deterministic and self-contained.
- `dump.sql` intentionally includes `TRUNCATE TABLE` statements to exercise the importer's destructive-statement handling and logging.
