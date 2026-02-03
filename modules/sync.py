from __future__ import annotations

import logging
from typing import Iterable, Optional

from .db import build_connection, select_database
from .parser import extract_insert_table, maybe_transform_statement, statement_splitter
from .types import ImportOptions


DEFAULT_TABLES = [
    "dashboard",
    "dashboard_version",
    "dashboard_acl",
    "folder",
    "data_source",
    "alert_rule",
]


def _columns_for_table(conn, db: str, table: str) -> list[str]:
    # This code here grabs column order so diff/apply are deterministic.
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            SELECT COLUMN_NAME
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s
            ORDER BY ORDINAL_POSITION
            """,
            (db, table),
        )
        return [row[0] for row in cursor.fetchall()]
    finally:
        cursor.close()


def _has_column(conn, db: str, table: str, column: str) -> bool:
    return column in _columns_for_table(conn, db, table)


def _key_columns(conn, db: str, table: str) -> list[str]:
    # This code here chooses natural keys; safer across environments.
    if table == "dashboard":
        return ["uid"]
    if table == "folder":
        return ["uid"]
    if table == "data_source":
        if _has_column(conn, db, table, "uid"):
            return ["uid"]
        return ["name"]
    if table == "alert_rule":
        if _has_column(conn, db, table, "uid"):
            return ["uid"]
        return ["id"]
    if table == "dashboard_version":
        return ["dashboard_id", "version"]
    if table == "dashboard_acl":
        cols = _columns_for_table(conn, db, table)
        keys = ["dashboard_id"]
        for col in ("user_id", "team_id", "role", "permission"):
            if col in cols:
                keys.append(col)
        return keys
    return ["id"]


def _rewrite_insert_table(statement: str, new_table: str) -> Optional[str]:
    # This code here swaps the target table in an INSERT/REPLACE statement.
    import re

    pattern = re.compile(
        r"^(INSERT\s+INTO|REPLACE\s+INTO|REPLACE)\s+([^\s(]+)(.*)$",
        re.IGNORECASE | re.DOTALL,
    )
    match = pattern.match(statement.lstrip())
    if not match:
        return None
    return f"{match.group(1)} {new_table}{match.group(3)}"


def _ensure_stage_tables(conn, target_db: str, stage_db: str, tables: list[str]) -> None:
    # This code here builds and resets the staging tables.
    cursor = conn.cursor()
    try:
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{stage_db}`")
        for table in tables:
            cursor.execute(f"CREATE TABLE IF NOT EXISTS `{stage_db}`.`{table}` LIKE `{target_db}`.`{table}`")
            cursor.execute(f"TRUNCATE TABLE `{stage_db}`.`{table}`")
    finally:
        cursor.close()


def _map_foreign_keys(conn, target_db: str, stage_db: str, tables: list[str]) -> None:
    # This code here remaps IDs in the stage DB using natural keys where possible.
    cursor = conn.cursor()
    try:
        if "dashboard" in tables and "folder" in tables:
            cursor.execute(
                f"""
                UPDATE `{stage_db}`.`dashboard` sd
                JOIN `{stage_db}`.`folder` sf ON sd.folder_id = sf.id
                JOIN `{target_db}`.`folder` tf ON sf.uid = tf.uid
                SET sd.folder_id = tf.id
                """
            )

        if "dashboard" in tables and "dashboard_version" in tables:
            cursor.execute(
                f"""
                UPDATE `{stage_db}`.`dashboard_version` sv
                JOIN `{stage_db}`.`dashboard` sd ON sv.dashboard_id = sd.id
                JOIN `{target_db}`.`dashboard` td ON sd.uid = td.uid
                SET sv.dashboard_id = td.id
                """
            )

        if "dashboard" in tables and "dashboard_acl" in tables:
            cursor.execute(
                f"""
                UPDATE `{stage_db}`.`dashboard_acl` sa
                JOIN `{stage_db}`.`dashboard` sd ON sa.dashboard_id = sd.id
                JOIN `{target_db}`.`dashboard` td ON sd.uid = td.uid
                SET sa.dashboard_id = td.id
                """
            )
    finally:
        cursor.close()


def _checksum_expr(columns: list[str], alias: str) -> str:
    # This code here builds a stable checksum across columns (NULL-safe).
    parts = [f"IFNULL(CAST({alias}.`{c}` AS CHAR), '__NULL__')" for c in columns]
    return f"MD5(CONCAT_WS('||', {', '.join(parts)}))"


def _diff_table(conn, target_db: str, stage_db: str, table: str, keys: list[str]) -> dict:
    # This code here computes new/changed/unchanged counts for a table.
    cursor = conn.cursor()
    try:
        cursor.execute(f"SELECT COUNT(*) FROM `{stage_db}`.`{table}`")
        stage_total = cursor.fetchone()[0]

        key_join = " AND ".join([f"t.`{k}` = s.`{k}`" for k in keys])
        cursor.execute(
            f"""
            SELECT COUNT(*)
            FROM `{stage_db}`.`{table}` s
            LEFT JOIN `{target_db}`.`{table}` t ON {key_join}
            WHERE t.`{keys[0]}` IS NULL
            """
        )
        new_rows = cursor.fetchone()[0]

        cols = _columns_for_table(conn, target_db, table)
        non_keys = [c for c in cols if c not in keys]
        if non_keys:
            s_hash = _checksum_expr(non_keys, "s")
            t_hash = _checksum_expr(non_keys, "t")
            cursor.execute(
                f"""
                SELECT COUNT(*)
                FROM `{stage_db}`.`{table}` s
                JOIN `{target_db}`.`{table}` t ON {key_join}
                WHERE {s_hash} <> {t_hash}
                """
            )
            changed = cursor.fetchone()[0]
        else:
            changed = 0

        unchanged = max(stage_total - new_rows - changed, 0)
        return {
            "table": table,
            "stage_total": stage_total,
            "new": new_rows,
            "changed": changed,
            "unchanged": unchanged,
        }
    finally:
        cursor.close()


def _apply_table(conn, target_db: str, stage_db: str, table: str, keys: list[str]) -> None:
    # This code here applies upserts using natural key joins.
    cursor = conn.cursor()
    try:
        cols = _columns_for_table(conn, target_db, table)
        non_keys = [c for c in cols if c not in keys]
        key_join = " AND ".join([f"t.`{k}` = s.`{k}`" for k in keys])

        cursor.execute(
            f"""
            INSERT INTO `{target_db}`.`{table}` ({', '.join(f'`{c}`' for c in cols)})
            SELECT {', '.join(f's.`{c}`' for c in cols)}
            FROM `{stage_db}`.`{table}` s
            LEFT JOIN `{target_db}`.`{table}` t ON {key_join}
            WHERE t.`{keys[0]}` IS NULL
            """
        )

        if non_keys:
            set_clause = ", ".join([f"t.`{c}` = s.`{c}`" for c in non_keys])
            cursor.execute(
                f"""
                UPDATE `{target_db}`.`{table}` t
                JOIN `{stage_db}`.`{table}` s ON {key_join}
                SET {set_clause}
                """
            )
    finally:
        cursor.close()


def sync_from_dump(
    opts: ImportOptions,
    dump_file: str,
    target_db: str,
    stage_db: str,
    tables: list[str],
    diff_only: bool,
    apply: bool,
) -> list[dict]:
    # This code here stages data, diffs it, and optionally applies updates.
    conn = build_connection(opts)
    try:
        select_database(conn, target_db)
        _ensure_stage_tables(conn, target_db, stage_db, tables)

        with open(dump_file, "r", encoding="utf-8", errors="replace") as fp:
            for statement, _, _ in statement_splitter(fp):
                transformed = maybe_transform_statement(statement, opts)
                if transformed is None:
                    continue
                table = extract_insert_table(transformed)
                table_name = table.split(".")[-1] if table else None
                if table_name and table_name in tables:
                    rewritten = _rewrite_insert_table(transformed, f"`{stage_db}`.`{table_name}`")
                    if rewritten:
                        cursor = conn.cursor()
                        try:
                            cursor.execute(rewritten)
                        finally:
                            cursor.close()
                    continue
                else:
                    continue

        _map_foreign_keys(conn, target_db, stage_db, tables)

        results = []
        for table in tables:
            keys = _key_columns(conn, target_db, table)
            results.append(_diff_table(conn, target_db, stage_db, table, keys))

        if apply and not diff_only:
            for table in tables:
                keys = _key_columns(conn, target_db, table)
                logging.info("Applying upsert for table %s", table)
                _apply_table(conn, target_db, stage_db, table, keys)

        return results
    finally:
        conn.close()
