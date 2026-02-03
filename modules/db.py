from __future__ import annotations

from typing import Optional

from .types import ImportOptions

MYSQLCLIENT_AVAILABLE = False
PYMYSQL_AVAILABLE = False

try:
    import MySQLdb  # type: ignore
    from MySQLdb.constants import CLIENT as MYSQLCLIENT

    MYSQLCLIENT_AVAILABLE = True
except Exception:
    MYSQLCLIENT_AVAILABLE = False

try:
    import pymysql  # type: ignore
    from pymysql.constants import CLIENT as PYMYSQLCLIENT

    PYMYSQL_AVAILABLE = True
except Exception:
    PYMYSQL_AVAILABLE = False


def detect_driver(prefer_mysqlclient: bool = True) -> str:
    # This code here picks mysqlclient first, then PyMySQL as backup.
    if prefer_mysqlclient and MYSQLCLIENT_AVAILABLE:
        return "mysqlclient"
    if PYMYSQL_AVAILABLE:
        return "pymysql"
    if MYSQLCLIENT_AVAILABLE:
        return "mysqlclient"
    raise RuntimeError("No MySQL driver found. Install mysqlclient or PyMySQL.")


def build_connection(opts: ImportOptions):
    # This code here wires up the DB connection and SSL bits if provided.
    driver = detect_driver(prefer_mysqlclient=True)
    charset = opts.force_charset or "utf8mb4"
    ssl = None
    if not opts.ssl_disabled and (opts.ssl_ca or opts.ssl_cert or opts.ssl_key):
        ssl = {}
        if opts.ssl_ca:
            ssl["ca"] = opts.ssl_ca
        if opts.ssl_cert:
            ssl["cert"] = opts.ssl_cert
        if opts.ssl_key:
            ssl["key"] = opts.ssl_key

    if driver == "mysqlclient":
        kwargs = {
            "host": opts.host,
            "port": opts.port,
            "user": opts.user,
            "passwd": opts.password,
            "charset": charset,
            "use_unicode": True,
            "autocommit": opts.autocommit,
            "client_flag": MYSQLCLIENT.MULTI_STATEMENTS,
        }
        if ssl is not None:
            kwargs["ssl"] = ssl
        return MySQLdb.connect(**kwargs)

    if driver == "pymysql":
        kwargs = {
            "host": opts.host,
            "port": opts.port,
            "user": opts.user,
            "password": opts.password,
            "charset": charset,
            "autocommit": opts.autocommit,
            "client_flag": PYMYSQLCLIENT.MULTI_STATEMENTS,
        }
        if ssl is not None:
            kwargs["ssl"] = ssl
        return pymysql.connect(**kwargs)

    raise RuntimeError("No compatible driver.")


def ensure_database(conn, opts: ImportOptions) -> None:
    # This code here creates or recreates the target DB and switches to it.
    cursor = conn.cursor()
    try:
        if opts.recreate_db:
            cursor.execute(f"DROP DATABASE IF EXISTS `{opts.target_db}`")
            cursor.execute(
                f"CREATE DATABASE `{opts.target_db}` CHARACTER SET utf8mb4"
            )
        elif opts.create_db:
            cursor.execute(
                f"CREATE DATABASE IF NOT EXISTS `{opts.target_db}` CHARACTER SET utf8mb4"
            )
        cursor.execute(f"USE `{opts.target_db}`")
    finally:
        cursor.close()


def select_database(conn, db_name: str) -> None:
    # This code here switches to the target DB without creating anything.
    cursor = conn.cursor()
    try:
        cursor.execute(f"USE `{db_name}`")
    finally:
        cursor.close()


def apply_session_toggles(conn, opts: ImportOptions) -> tuple[Optional[int], Optional[int]]:
    # This code here flips session flags (FK/unique/sql_mode) and returns originals.
    cursor = conn.cursor()
    try:
        original_fk = None
        original_uniq = None

        if opts.disable_foreign_keys:
            cursor.execute("SELECT @@FOREIGN_KEY_CHECKS")
            original_fk = int(cursor.fetchone()[0])
            cursor.execute("SET FOREIGN_KEY_CHECKS=0")

        if opts.disable_unique_checks:
            cursor.execute("SELECT @@UNIQUE_CHECKS")
            original_uniq = int(cursor.fetchone()[0])
            cursor.execute("SET UNIQUE_CHECKS=0")

        if opts.sql_mode is not None:
            cursor.execute("SET SESSION sql_mode=%s", (opts.sql_mode,))

        return original_fk, original_uniq
    finally:
        cursor.close()


def restore_session_toggles(
    conn, original_fk: Optional[int], original_uniq: Optional[int]
) -> None:
    # This code here puts session flags back how we found them.
    cursor = conn.cursor()
    try:
        if original_fk is not None:
            cursor.execute("SET FOREIGN_KEY_CHECKS=%s", (original_fk,))
        if original_uniq is not None:
            cursor.execute("SET UNIQUE_CHECKS=%s", (original_uniq,))
    finally:
        cursor.close()
