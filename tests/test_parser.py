import unittest

from modules.parser import extract_insert_table, maybe_transform_statement, statement_splitter
from modules.types import ImportOptions, ParseError


def _opts(**overrides):
    base = dict(
        dump_file="/tmp/x.sql",
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
        quarantine_file="quarantine.sql",
        quarantine_all_failures=True,
        quarantine_only_inserts=False,
        fail_on_error=False,
        progress_mb=50,
        progress_statements=5000,
        progress_bar=False,
        progress_bar_logs=False,
        worker_progress=False,
        worker_progress_interval=5.0,
        auto_tune_batch=True,
        resume=False,
        resume_file="import.resume.json",
        log_file=None,
        cleanup_temp=False,
        purge_temp=True,
        ignore_locks=False,
        allow_delimiter=False,
        no_transforms=False,
        transform_insert_or_replace=True,
        parallel_per_table=False,
        parallel_workers=4,
        parallel_temp_dir="/tmp/grafana-import",
        dry_run=False,
        dry_run_parallel=False,
        ssl_ca=None,
        ssl_cert=None,
        ssl_key=None,
        ssl_disabled=False,
    )
    base.update(overrides)
    return ImportOptions(**base)


class StatementSplitterTests(unittest.TestCase):
    def test_splits_on_semicolons(self):
        sql = ["CREATE TABLE t (id INT);\n", "INSERT INTO t VALUES (1);\n"]
        stmts = list(statement_splitter(sql))
        self.assertEqual(len(stmts), 2)
        self.assertTrue(stmts[0][0].startswith("CREATE TABLE"))
        self.assertTrue(stmts[1][0].startswith("INSERT INTO"))

    def test_ignores_semicolons_in_quotes(self):
        sql = ["INSERT INTO t VALUES ('a;b');\n", "SELECT 1;\n"]
        stmts = list(statement_splitter(sql))
        self.assertEqual(len(stmts), 2)
        self.assertIn("'a;b'", stmts[0][0])

    def test_ignores_semicolons_in_backticks(self):
        sql = ["CREATE TABLE `a;b` (id INT);\n"]
        stmts = list(statement_splitter(sql))
        self.assertEqual(len(stmts), 1)
        self.assertIn("`a;b`", stmts[0][0])

    def test_handles_comments(self):
        sql = ["-- comment;\n", "SELECT 1;\n", "# c2;\n", "SELECT 2;\n"]
        stmts = list(statement_splitter(sql))
        self.assertEqual(len(stmts), 2)
        self.assertIn("SELECT 1", stmts[0][0])
        self.assertIn("SELECT 2", stmts[1][0])


class TransformTests(unittest.TestCase):
    def test_pragmas_are_skipped(self):
        opts = _opts()
        self.assertIsNone(maybe_transform_statement("PRAGMA journal_mode=WAL;", opts))

    def test_insert_or_replace_transformed(self):
        opts = _opts()
        out = maybe_transform_statement("INSERT OR REPLACE INTO t VALUES (1);", opts)
        self.assertTrue(out.startswith("REPLACE INTO"))

    def test_delimiter_requires_flag(self):
        opts = _opts(allow_delimiter=False)
        with self.assertRaises(ParseError):
            maybe_transform_statement("DELIMITER $$", opts)

    def test_extract_insert_table(self):
        self.assertEqual(
            extract_insert_table("INSERT INTO `foo` VALUES (1);"),
            "foo",
        )
        self.assertEqual(
            extract_insert_table("REPLACE INTO bar VALUES (1);"),
            "bar",
        )
        self.assertEqual(
            extract_insert_table("INSERT INTO db1.baz VALUES (1);"),
            "db1.baz",
        )


if __name__ == "__main__":
    unittest.main()
