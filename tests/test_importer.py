import os
import shutil
import unittest
from unittest.mock import patch

from modules.importer import (
    _coalesce_batch,
    _parallel_stage_dir,
    _parallel_stage_path,
    _ordered_table_items,
    _should_process_statement,
    import_dump,
    import_dump_parallel_per_table,
    process_batch,
)
from modules.types import ImportOptions


def _opts(**overrides):
    root = os.path.join(os.getcwd(), ".tmp_importer_tests")
    base = dict(
        dump_file=os.path.join(root, "dump.sql"),
        table_filter=(),
        host="127.0.0.1",
        port=3306,
        user="root",
        password="",
        target_db="grafana",
        commit_statements=500,
        commit_bytes=5 * 1024 * 1024,
        disable_foreign_keys=False,
        disable_unique_checks=False,
        sql_mode=None,
        autocommit=False,
        force_charset="utf8mb4",
        create_db=False,
        recreate_db=False,
        quarantine_file=os.path.join(root, "quarantine.sql"),
        quarantine_all_failures=True,
        quarantine_only_inserts=False,
        fail_on_error=False,
        progress_mb=0,
        progress_statements=0,
        progress_bar=False,
        progress_bar_logs=False,
        worker_progress=False,
        worker_progress_interval=5.0,
        log_file=None,
        auto_tune_batch=False,
        combine_inserts=False,
        combine_insert_group_size=25,
        resume=False,
        resume_file=os.path.join(root, "resume.json"),
        ignore_locks=False,
        allow_delimiter=False,
        no_transforms=False,
        transform_insert_or_replace=True,
        parallel_per_table=False,
        parallel_workers=1,
        parallel_temp_dir=os.path.join(root, "stage"),
        parallel_table_priority=(),
        verify_tables=(),
        dry_run=False,
        dry_run_parallel=False,
        cleanup_temp=False,
        purge_temp=True,
        ssl_ca=None,
        ssl_cert=None,
        ssl_key=None,
        ssl_disabled=False,
    )
    base.update(overrides)
    return ImportOptions(**base)


class FakeCursor:
    def __init__(self, conn):
        self.conn = conn
        self.last_statement = None

    def execute(self, statement, params=None):
        self.last_statement = statement
        self.conn.executed.append(statement)

    def fetchone(self):
        if self.last_statement == "SELECT @@hostname, @@server_uuid, @@read_only, DATABASE()":
            return ("db-host", "uuid-1", 0, "grafana")
        return (1,)

    def close(self):
        pass


class FakeConn:
    def __init__(self):
        self.executed = []

    def cursor(self):
        return FakeCursor(self)

    def begin(self):
        self.executed.append("BEGIN")

    def commit(self):
        self.executed.append("COMMIT")

    def rollback(self):
        self.executed.append("ROLLBACK")

    def close(self):
        pass


class ExplodingConn(FakeConn):
    def cursor(self):
        return ExplodingCursor(self)


class ExplodingCursor(FakeCursor):
    def execute(self, statement, params=None):
        self.conn.executed.append(statement)
        if "INSERT INTO annotation" in statement:
            raise RuntimeError("boom")


class ImporterBatchReplayTests(unittest.TestCase):
    def setUp(self):
        self.root = os.path.join(os.getcwd(), ".tmp_importer_tests")
        shutil.rmtree(self.root, ignore_errors=True)
        os.makedirs(self.root)
        os.makedirs(os.path.join(self.root, "stage"))

    def tearDown(self):
        shutil.rmtree(self.root, ignore_errors=True)

    def test_linear_final_batch_not_replayed_in_finally(self):
        dump_file = os.path.join(self.root, "dump.sql")
        with open(dump_file, "w", encoding="utf-8") as fp:
            fp.write("TRUNCATE TABLE annotation;\n")

        conn = FakeConn()
        opts = _opts(dump_file=dump_file)

        with patch("modules.importer.build_connection", return_value=conn), patch(
            "modules.importer.ensure_database", lambda conn, opts: None
        ), patch(
            "modules.importer.apply_session_toggles", return_value=(None, None)
        ), patch(
            "modules.importer.restore_session_toggles", lambda conn, fk, uq: None
        ):
            import_dump(opts)

        self.assertEqual(conn.executed.count("TRUNCATE TABLE annotation;"), 1)

    def test_parallel_final_non_insert_batch_not_replayed_in_finally(self):
        dump_file = os.path.join(self.root, "dump.sql")
        with open(dump_file, "w", encoding="utf-8") as fp:
            fp.write("TRUNCATE TABLE annotation;\n")
            fp.write("INSERT INTO annotation VALUES (1);\n")

        main_conn = FakeConn()
        worker_conn = FakeConn()
        opts = _opts(dump_file=dump_file, parallel_per_table=True)

        with patch(
            "modules.importer.build_connection",
            side_effect=[main_conn, worker_conn],
        ), patch(
            "modules.importer.ensure_database", lambda conn, opts: None
        ), patch(
            "modules.importer.apply_session_toggles", return_value=(None, None)
        ), patch(
            "modules.importer.restore_session_toggles", lambda conn, fk, uq: None
        ):
            import_dump_parallel_per_table(opts)

        self.assertEqual(main_conn.executed.count("TRUNCATE TABLE annotation;"), 1)


class ParallelPriorityTests(unittest.TestCase):
    def test_priority_tables_queue_first(self):
        ordered = _ordered_table_items(
            {
                "annotation": "/tmp/a.sql",
                "dashboard": "/tmp/d.sql",
                "org": "/tmp/o.sql",
                "user": "/tmp/u.sql",
            },
            ("org", "user"),
        )
        self.assertEqual(
            [name for name, _ in ordered],
            ["org", "user", "annotation", "dashboard"],
        )

    def test_default_priority_prefers_core_parent_tables(self):
        ordered = _ordered_table_items(
            {
                "annotation": "/tmp/a.sql",
                "dashboard": "/tmp/d.sql",
                "org": "/tmp/o.sql",
                "user": "/tmp/u.sql",
            },
            (),
        )
        self.assertEqual(
            [name for name, _ in ordered],
            ["org", "user", "dashboard", "annotation"],
        )


class ParallelSafetyTests(unittest.TestCase):
    def setUp(self):
        self.root = os.path.join(os.getcwd(), ".tmp_importer_tests")
        shutil.rmtree(self.root, ignore_errors=True)
        os.makedirs(self.root)
        os.makedirs(os.path.join(self.root, "stage"))

    def tearDown(self):
        shutil.rmtree(self.root, ignore_errors=True)

    def test_dry_run_parallel_stages_under_tables_subdir(self):
        dump_file = os.path.join(self.root, "dump.sql")
        with open(dump_file, "w", encoding="utf-8") as fp:
            fp.write("INSERT INTO annotation VALUES (1);\n")

        opts = _opts(
            dump_file=dump_file,
            dry_run=True,
            dry_run_parallel=True,
            parallel_temp_dir=os.path.join(self.root, "stage"),
        )
        import_dump(opts)
        self.assertTrue(os.path.exists(_parallel_stage_path(opts.parallel_temp_dir, "annotation")))

    def test_parallel_worker_error_is_raised(self):
        dump_file = os.path.join(self.root, "dump.sql")
        with open(dump_file, "w", encoding="utf-8") as fp:
            fp.write("INSERT INTO annotation VALUES (1);\n")

        main_conn = FakeConn()
        worker_conn = FakeConn()
        opts = _opts(dump_file=dump_file, parallel_per_table=True)

        with patch(
            "modules.importer.build_connection",
            side_effect=[main_conn, worker_conn],
        ), patch(
            "modules.importer.ensure_database", lambda conn, opts: None
        ), patch(
            "modules.importer.apply_session_toggles", return_value=(None, None)
        ), patch(
            "modules.importer.restore_session_toggles", lambda conn, fk, uq: None
        ), patch(
            "modules.importer.process_batch", side_effect=RuntimeError("boom")
        ):
            with self.assertRaises(RuntimeError):
                import_dump_parallel_per_table(opts)

    def test_parallel_worker_applies_session_toggles(self):
        dump_file = os.path.join(self.root, "dump.sql")
        with open(dump_file, "w", encoding="utf-8") as fp:
            fp.write("INSERT INTO annotation VALUES (1);\n")

        main_conn = FakeConn()
        worker_conn = FakeConn()
        opts = _opts(dump_file=dump_file, parallel_per_table=True)
        toggle_calls = []

        def fake_toggle(conn, _opts):
            toggle_calls.append(conn)
            return (None, None)

        with patch(
            "modules.importer.build_connection",
            side_effect=[main_conn, worker_conn],
        ), patch(
            "modules.importer.ensure_database", lambda conn, opts: None
        ), patch(
            "modules.importer.apply_session_toggles", side_effect=fake_toggle
        ), patch(
            "modules.importer.restore_session_toggles", lambda conn, fk, uq: None
        ):
            import_dump_parallel_per_table(opts)

        self.assertEqual(toggle_calls, [main_conn, worker_conn])


class ImporterFilterTests(unittest.TestCase):
    def test_table_filter_accepts_matching_insert(self):
        opts = _opts(table_filter=("annotation",))
        self.assertTrue(
            _should_process_statement("INSERT INTO annotation VALUES (1);", opts)
        )

    def test_table_filter_skips_non_matching_insert_and_non_insert_sql(self):
        opts = _opts(table_filter=("annotation",))
        self.assertFalse(
            _should_process_statement("INSERT INTO dashboard VALUES (1);", opts)
        )
        self.assertFalse(
            _should_process_statement("TRUNCATE TABLE annotation;", opts)
        )


class InsertCoalescingTests(unittest.TestCase):
    def setUp(self):
        self.root = os.path.join(os.getcwd(), ".tmp_importer_tests")
        shutil.rmtree(self.root, ignore_errors=True)
        os.makedirs(self.root)

    def tearDown(self):
        shutil.rmtree(self.root, ignore_errors=True)

    def test_coalesce_batch_merges_consecutive_matching_inserts(self):
        opts = _opts(combine_inserts=True, combine_insert_group_size=10)
        batch = [
            ("INSERT INTO annotation (id, text) VALUES (1, 'a');", 1, 1),
            ("INSERT INTO annotation (id, text) VALUES (2, 'b');", 2, 2),
            ("INSERT INTO dashboard (id) VALUES (3);", 3, 3),
        ]

        grouped = _coalesce_batch(batch, opts)

        self.assertEqual(len(grouped), 2)
        self.assertEqual(
            grouped[0][0][0],
            "INSERT INTO annotation (id, text) VALUES (1, 'a'),(2, 'b');",
        )
        self.assertEqual(len(grouped[0][1]), 2)
        self.assertEqual(grouped[1][0][0], "INSERT INTO dashboard (id) VALUES (3);")

    def test_process_batch_falls_back_to_individual_inserts_after_merged_failure(self):
        class FlakyCursor(FakeCursor):
            def execute(self, statement, params=None):
                self.last_statement = statement
                self.conn.executed.append(statement)
                if statement == "INSERT INTO annotation (id, text) VALUES (1, 'a'),(2, 'b');":
                    raise RuntimeError("merged boom")

        class FlakyConn(FakeConn):
            def cursor(self):
                return FlakyCursor(self)

        opts = _opts(combine_inserts=True, combine_insert_group_size=10)
        conn = FlakyConn()
        batch = [
            ("INSERT INTO annotation (id, text) VALUES (1, 'a');", 1, 1),
            ("INSERT INTO annotation (id, text) VALUES (2, 'b');", 2, 2),
        ]
        quarantine_path = os.path.join(self.root, "quarantine.sql")

        class BatchStats:
            statements_ok = 0
            statements_failed = 0

        with open(quarantine_path, "w", encoding="utf-8") as quarantine_fp:
            ok, failed = process_batch(conn, batch, BatchStats(), opts, quarantine_fp)

        self.assertEqual((ok, failed), (2, 0))
        self.assertIn(
            "INSERT INTO annotation (id, text) VALUES (1, 'a'),(2, 'b');",
            conn.executed,
        )
        self.assertIn(
            "INSERT INTO annotation (id, text) VALUES (1, 'a');",
            conn.executed,
        )
        self.assertIn(
            "INSERT INTO annotation (id, text) VALUES (2, 'b');",
            conn.executed,
        )


if __name__ == "__main__":
    unittest.main()
