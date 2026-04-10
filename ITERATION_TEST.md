# Combined Insert Iteration Test

This fixture is for manually validating the `--combine-inserts` fallback path.

Files:

- [bad_insert_iteration.sql](/home/tearle/Work/sql_dump/git-grafana/grafana-sqlite-to-mysql/bad_insert_iteration.sql)

Expected behavior with `--combine-inserts --combine-insert-group-size 25`:

1. The four consecutive `org` inserts are merged into one multi-row statement.
2. That merged statement fails because one row duplicates `org.id = 1`.
3. The importer falls back to the original `org` statements one by one.
4. The good `org` rows insert successfully.
5. The duplicate `org` row is quarantined.
6. The later `team` insert still runs successfully.

Expected final counts:

- `org = 3`
- `team = 1`
