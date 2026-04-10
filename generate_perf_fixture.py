#!/usr/bin/env python3
"""Generate a large synthetic MySQL import fixture for performance testing."""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class TableSpec:
    name: str
    columns: tuple[str, ...]
    weight: int
    payload_bytes: int


TABLE_SPECS = (
    TableSpec("org", ("id", "name", "slug", "created_ms"), 1, 48),
    TableSpec("user_account", ("id", "org_id", "login", "email", "display_name", "prefs_json"), 2, 160),
    TableSpec("team", ("id", "org_id", "name", "email"), 1, 96),
    TableSpec("folder", ("id", "org_id", "uid", "title", "description"), 1, 160),
    TableSpec("dashboard", ("id", "org_id", "folder_id", "uid", "title", "data_json"), 3, 4096),
    TableSpec("dashboard_version", ("id", "dashboard_id", "version_num", "message", "data_json"), 5, 8192),
    TableSpec("data_source", ("id", "org_id", "uid", "name", "ds_type", "config_json"), 1, 768),
    TableSpec("annotation", ("id", "org_id", "dashboard_id", "user_id", "epoch_ms", "text_blob", "data_json"), 5, 2048),
    TableSpec("annotation_tag", ("id", "annotation_id", "tag"), 2, 64),
    TableSpec("alert_rule", ("id", "org_id", "uid", "title", "condition_json"), 2, 3072),
    TableSpec("alert_rule_version", ("id", "rule_id", "version_num", "message", "definition_json"), 4, 6144),
    TableSpec("event_log", ("id", "org_id", "user_id", "action", "payload_json"), 4, 1024),
    TableSpec("library_element", ("id", "org_id", "uid", "name", "kind", "model_json"), 2, 2048),
)


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS org (
  id BIGINT PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  slug VARCHAR(255) NOT NULL,
  created_ms BIGINT NOT NULL,
  KEY idx_org_slug (slug)
);

CREATE TABLE IF NOT EXISTS user_account (
  id BIGINT PRIMARY KEY,
  org_id BIGINT NOT NULL,
  login VARCHAR(255) NOT NULL,
  email VARCHAR(255) NOT NULL,
  display_name VARCHAR(255) NOT NULL,
  prefs_json LONGTEXT NOT NULL,
  KEY idx_user_org (org_id)
);

CREATE TABLE IF NOT EXISTS team (
  id BIGINT PRIMARY KEY,
  org_id BIGINT NOT NULL,
  name VARCHAR(255) NOT NULL,
  email VARCHAR(255) NOT NULL,
  KEY idx_team_org (org_id)
);

CREATE TABLE IF NOT EXISTS folder (
  id BIGINT PRIMARY KEY,
  org_id BIGINT NOT NULL,
  uid VARCHAR(40) NOT NULL,
  title VARCHAR(255) NOT NULL,
  description TEXT NOT NULL,
  KEY idx_folder_org (org_id)
);

CREATE TABLE IF NOT EXISTS dashboard (
  id BIGINT PRIMARY KEY,
  org_id BIGINT NOT NULL,
  folder_id BIGINT NOT NULL,
  uid VARCHAR(40) NOT NULL,
  title VARCHAR(255) NOT NULL,
  data_json LONGTEXT NOT NULL,
  KEY idx_dashboard_org (org_id),
  KEY idx_dashboard_folder (folder_id)
);

CREATE TABLE IF NOT EXISTS dashboard_version (
  id BIGINT PRIMARY KEY,
  dashboard_id BIGINT NOT NULL,
  version_num INT NOT NULL,
  message VARCHAR(255) NOT NULL,
  data_json LONGTEXT NOT NULL,
  KEY idx_dashboard_version_dashboard (dashboard_id)
);

CREATE TABLE IF NOT EXISTS data_source (
  id BIGINT PRIMARY KEY,
  org_id BIGINT NOT NULL,
  uid VARCHAR(40) NOT NULL,
  name VARCHAR(255) NOT NULL,
  ds_type VARCHAR(64) NOT NULL,
  config_json LONGTEXT NOT NULL,
  KEY idx_data_source_org (org_id)
);

CREATE TABLE IF NOT EXISTS annotation (
  id BIGINT PRIMARY KEY,
  org_id BIGINT NOT NULL,
  dashboard_id BIGINT NOT NULL,
  user_id BIGINT NOT NULL,
  epoch_ms BIGINT NOT NULL,
  text_blob LONGTEXT NOT NULL,
  data_json LONGTEXT NOT NULL,
  KEY idx_annotation_org (org_id),
  KEY idx_annotation_dashboard (dashboard_id),
  KEY idx_annotation_user (user_id)
);

CREATE TABLE IF NOT EXISTS annotation_tag (
  id BIGINT PRIMARY KEY,
  annotation_id BIGINT NOT NULL,
  tag VARCHAR(255) NOT NULL,
  KEY idx_annotation_tag_annotation (annotation_id)
);

CREATE TABLE IF NOT EXISTS alert_rule (
  id BIGINT PRIMARY KEY,
  org_id BIGINT NOT NULL,
  uid VARCHAR(40) NOT NULL,
  title VARCHAR(255) NOT NULL,
  condition_json LONGTEXT NOT NULL,
  KEY idx_alert_rule_org (org_id)
);

CREATE TABLE IF NOT EXISTS alert_rule_version (
  id BIGINT PRIMARY KEY,
  rule_id BIGINT NOT NULL,
  version_num INT NOT NULL,
  message VARCHAR(255) NOT NULL,
  definition_json LONGTEXT NOT NULL,
  KEY idx_alert_rule_version_rule (rule_id)
);

CREATE TABLE IF NOT EXISTS event_log (
  id BIGINT PRIMARY KEY,
  org_id BIGINT NOT NULL,
  user_id BIGINT NOT NULL,
  action VARCHAR(255) NOT NULL,
  payload_json LONGTEXT NOT NULL,
  KEY idx_event_log_org (org_id),
  KEY idx_event_log_user (user_id)
);

CREATE TABLE IF NOT EXISTS library_element (
  id BIGINT PRIMARY KEY,
  org_id BIGINT NOT NULL,
  uid VARCHAR(40) NOT NULL,
  name VARCHAR(255) NOT NULL,
  kind VARCHAR(64) NOT NULL,
  model_json LONGTEXT NOT NULL,
  KEY idx_library_element_org (org_id)
);
""".strip() + "\n"


def _fill(label: str, row_id: int, target_size: int) -> str:
    prefix = f"{label}-{row_id}-"
    payload = (prefix + ("x" * max(0, target_size - len(prefix))))[:target_size]
    return "'" + payload + "'"


def _json_blob(kind: str, row_id: int, target_size: int) -> str:
    prefix = f'{{"kind":"{kind}","row":{row_id},"payload":"'
    suffix = '"}'
    payload_len = max(0, target_size - len(prefix) - len(suffix))
    payload = ("p" * payload_len)
    return "'" + prefix + payload + suffix + "'"


def _table_values(spec: TableSpec, row_id: int, counters: dict[str, int]) -> tuple[str, ...]:
    org_id = max(1, ((row_id - 1) % max(1, counters["org"])) + 1)
    user_id = max(1, ((row_id - 1) % max(1, counters["user_account"])) + 1)
    folder_id = max(1, ((row_id - 1) % max(1, counters["folder"])) + 1)
    dashboard_id = max(1, ((row_id - 1) % max(1, counters["dashboard"])) + 1)
    annotation_id = max(1, ((row_id - 1) % max(1, counters["annotation"])) + 1)
    rule_id = max(1, ((row_id - 1) % max(1, counters["alert_rule"])) + 1)
    if spec.name == "org":
        return (str(row_id), _fill("org", row_id, 24), _fill("slug", row_id, 18), str(1_700_000_000_000 + row_id))
    if spec.name == "user_account":
        return (
            str(row_id),
            str(org_id),
            _fill("login", row_id, 24),
            _fill("email", row_id, 32),
            _fill("display", row_id, 28),
            _json_blob("prefs", row_id, spec.payload_bytes),
        )
    if spec.name == "team":
        return (str(row_id), str(org_id), _fill("team", row_id, 24), _fill("team-email", row_id, 32))
    if spec.name == "folder":
        return (str(row_id), str(org_id), _fill("folder-uid", row_id, 20), _fill("folder-title", row_id, 40), _fill("folder-desc", row_id, spec.payload_bytes))
    if spec.name == "dashboard":
        return (
            str(row_id),
            str(org_id),
            str(folder_id),
            _fill("dash-uid", row_id, 20),
            _fill("dashboard-title", row_id, 48),
            _json_blob("dashboard", row_id, spec.payload_bytes),
        )
    if spec.name == "dashboard_version":
        return (
            str(row_id),
            str(dashboard_id),
            str((row_id % 50) + 1),
            _fill("version-message", row_id, 48),
            _json_blob("dashboard-version", row_id, spec.payload_bytes),
        )
    if spec.name == "data_source":
        return (
            str(row_id),
            str(org_id),
            _fill("ds-uid", row_id, 20),
            _fill("datasource-name", row_id, 40),
            _fill("datasource-type", row_id, 16),
            _json_blob("data-source", row_id, spec.payload_bytes),
        )
    if spec.name == "annotation":
        return (
            str(row_id),
            str(org_id),
            str(dashboard_id),
            str(user_id),
            str(1_700_000_000_000 + row_id),
            _fill("annotation-text", row_id, spec.payload_bytes // 3),
            _json_blob("annotation", row_id, spec.payload_bytes),
        )
    if spec.name == "annotation_tag":
        return (str(row_id), str(annotation_id), _fill("tag", row_id, 32))
    if spec.name == "alert_rule":
        return (
            str(row_id),
            str(org_id),
            _fill("alert-rule-uid", row_id, 20),
            _fill("alert-rule-title", row_id, 48),
            _json_blob("alert-rule", row_id, spec.payload_bytes),
        )
    if spec.name == "alert_rule_version":
        return (
            str(row_id),
            str(rule_id),
            str((row_id % 20) + 1),
            _fill("alert-version-message", row_id, 48),
            _json_blob("alert-rule-version", row_id, spec.payload_bytes),
        )
    if spec.name == "event_log":
        return (
            str(row_id),
            str(org_id),
            str(user_id),
            _fill("event-action", row_id, 24),
            _json_blob("event-log", row_id, spec.payload_bytes),
        )
    if spec.name == "library_element":
        return (
            str(row_id),
            str(org_id),
            _fill("library-uid", row_id, 20),
            _fill("library-name", row_id, 40),
            _fill("library-kind", row_id, 16),
            _json_blob("library-element", row_id, spec.payload_bytes),
        )
    raise ValueError(f"Unknown table: {spec.name}")


def generate_fixture(output_dir: Path, target_bytes: int, rows_per_insert: int) -> dict[str, object]:
    output_dir.mkdir(parents=True, exist_ok=True)
    schema_path = output_dir / "schema.sql"
    dump_path = output_dir / "dump.sql"
    manifest_path = output_dir / "manifest.json"
    schema_path.write_text(SCHEMA_SQL, encoding="utf-8")

    counters = {spec.name: 0 for spec in TABLE_SPECS}
    weighted_specs = [spec for spec in TABLE_SPECS for _ in range(spec.weight)]
    bytes_written = 0
    statements = 0

    with dump_path.open("w", encoding="utf-8") as fp:
        header = [
            "-- Synthetic performance dump generated by generate_perf_fixture.py\n",
            "SET FOREIGN_KEY_CHECKS=0;\n",
        ]
        for line in header:
            fp.write(line)
            bytes_written += len(line.encode("utf-8"))

        for spec in TABLE_SPECS:
            stmt = f"TRUNCATE TABLE {spec.name};\n"
            fp.write(stmt)
            bytes_written += len(stmt.encode("utf-8"))
            statements += 1

        while bytes_written < target_bytes:
            for spec in weighted_specs:
                rows = []
                for _ in range(rows_per_insert):
                    row_id = counters[spec.name] + 1
                    rows.append("(" + ",".join(_table_values(spec, row_id, counters)) + ")")
                    counters[spec.name] = row_id
                stmt = (
                    f"INSERT INTO {spec.name} ({', '.join(spec.columns)}) VALUES "
                    + ",".join(rows)
                    + ";\n"
                )
                fp.write(stmt)
                bytes_written += len(stmt.encode("utf-8"))
                statements += 1
                if bytes_written >= target_bytes:
                    break

        footer = "SET FOREIGN_KEY_CHECKS=1;\n"
        fp.write(footer)
        bytes_written += len(footer.encode("utf-8"))

    manifest = {
        "target_bytes": target_bytes,
        "actual_bytes": dump_path.stat().st_size,
        "rows_per_insert": rows_per_insert,
        "statements": statements,
        "tables": {name: counters[name] for name in counters},
    }
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return manifest


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate a synthetic SQL fixture for importer performance testing.")
    parser.add_argument("--output-dir", default=".perf-fixture", help="Directory to write schema.sql, dump.sql, and manifest.json")
    parser.add_argument("--target-size-mib", type=int, default=1024, help="Approximate dump size to generate in MiB")
    parser.add_argument("--rows-per-insert", type=int, default=10, help="Rows per INSERT statement")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.target_size_mib <= 0:
        parser.error("--target-size-mib must be positive")
    if args.rows_per_insert <= 0:
        parser.error("--rows-per-insert must be positive")

    target_bytes = args.target_size_mib * 1024 * 1024
    output_dir = Path(args.output_dir)
    manifest = generate_fixture(output_dir, target_bytes, args.rows_per_insert)

    print(f"Wrote fixture to {output_dir}")
    print(f"schema: {output_dir / 'schema.sql'}")
    print(f"dump:   {output_dir / 'dump.sql'}")
    print(f"size:   {manifest['actual_bytes']} bytes")
    print(f"rows-per-insert: {manifest['rows_per_insert']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
