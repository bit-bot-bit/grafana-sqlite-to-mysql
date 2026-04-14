# Performance Smoke Report

Date: 2026-04-09

## Scope

Validated the new synthetic performance harness end-to-end with a reduced dataset before scaling to `1 GiB`.

## Fixture generation

Command:

```bash
python3 generate_perf_fixture.py --output-dir .tmp_perf_smoke --target-size-mib 2 --rows-per-insert 5
```

Result:

- `schema.sql` generated
- `dump.sql` generated
- `manifest.json` generated
- dump size: `2,120,365` bytes

## Import smoke run

Environment:

- `podman compose -f docker-compose.perf.yml`
- MySQL 8 container
- verifier container running the importer

Importer result:

- mode: live import
- target DB: `grafana_perf`
- parallel per table: enabled
- workers: `4`
- completed import: `140` statements
- succeeded: `140`
- failed: `0`
- runtime: about `0.6s`

## Post-import row counts

- `org`: `20`
- `dashboard`: `60`
- `annotation`: `100`
- `alert_rule_version`: `75`

## Notes

- The original perf verifier image needed manual package installs for `PyMySQL` and `cryptography`.
- `docker-compose.perf.yml` was updated to build from `Dockerfile.perf-verifier`, so the perf harness is now self-contained.
- After rebuilding the stack, the documented sequence was revalidated successfully:
  1. load `schema.sql`
  2. run the importer
  3. confirm row counts in MySQL
