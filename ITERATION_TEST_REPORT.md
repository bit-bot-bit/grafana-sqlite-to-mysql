# Combined Insert Fallback Report

Date: 2026-04-09

## Fixture

- [bad_insert_iteration.sql](/home/tearle/Work/sql_dump/git-grafana/grafana-sqlite-to-mysql/bad_insert_iteration.sql)

The fixture contains:

- four consecutive `org` inserts
- one duplicate primary-key row inside that `org` group
- one later `team` insert

## Import command

```bash
podman compose -f docker-compose.perf.yml exec verifier bash -lc "
  cd /workspace && rm -f iteration_quarantine.sql && \
  python3 import_grafana_dump.py \
    --dump-file bad_insert_iteration.sql \
    --target-db grafana_iter \
    --host mysql \
    --port 3306 \
    --user root \
    --password rootpass \
    --combine-inserts \
    --combine-insert-group-size 25 \
    --verify-tables org,team \
    --quarantine-file iteration_quarantine.sql \
    --yes"
```

## Import result

- completed import: `total=7 ok=6 failed=1`
- one duplicate-key failure was logged for the bad `org` row
- import completed successfully without aborting

## Verified database result

- `org_count = 3`
- `team_count = 1`

Rows present:

- `org`: ids `1`, `2`, `3`
- `team`: id `10`, `org_id = 1`

## Quarantine result

`iteration_quarantine.sql` contains only the duplicate row:

```sql
INSERT INTO org (id, name, slug, created_ms) VALUES (1, 'org-dup', 'slug-dup', 1700000000999);
```

## Conclusion

The combined-insert fallback logic behaved as intended:

1. the compatible `org` inserts were grouped
2. the merged group failed because of the duplicate primary key
3. the importer fell back to the original statements inside that group
4. the valid `org` rows were inserted
5. the bad `org` row was quarantined
6. the later `team` insert still succeeded
