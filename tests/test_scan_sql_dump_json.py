import os
import shutil
import unittest
from contextlib import redirect_stdout
from io import StringIO

from scan_sql_dump_json import main, scan_dump


class ScanSqlDumpJsonTests(unittest.TestCase):
    def setUp(self):
        self.root = os.path.join(os.getcwd(), ".tmp_scan_json")
        shutil.rmtree(self.root, ignore_errors=True)
        os.makedirs(self.root)

    def tearDown(self):
        shutil.rmtree(self.root, ignore_errors=True)

    def test_flags_invalid_json_in_dashboard_data_column(self):
        dump_file = os.path.join(self.root, "dump.sql")
        with open(dump_file, "w", encoding="utf-8") as fp:
            fp.write(
                "INSERT INTO dashboard (id, uid, data) VALUES "
                "(1, 'abc123', '{\"title\":\"bad\",}');\n"
            )

        findings = scan_dump(dump_file, (), set())

        self.assertEqual(len(findings), 1)
        self.assertEqual(findings[0].code, "invalid_json_payload")
        self.assertEqual(findings[0].table, "dashboard")
        self.assertEqual(findings[0].column, "data")

    def test_accepts_valid_json_after_sql_string_decoding(self):
        dump_file = os.path.join(self.root, "dump.sql")
        with open(dump_file, "w", encoding="utf-8") as fp:
            fp.write(
                "INSERT INTO alert_rule (id, title, condition_json) VALUES "
                "(1, 'ok', '{\"expr\":\"path \\\\\\\\ server\"}');\n"
            )

        findings = scan_dump(dump_file, (), set())

        self.assertEqual(findings, [])

    def test_can_force_check_nonstandard_column_name(self):
        dump_file = os.path.join(self.root, "dump.sql")
        with open(dump_file, "w", encoding="utf-8") as fp:
            fp.write(
                "INSERT INTO weird_table (id, content) VALUES "
                "(1, '{\"bad\":]');\n"
            )

        findings = scan_dump(dump_file, (), {"content"})

        self.assertEqual(len(findings), 1)
        self.assertEqual(findings[0].column, "content")

    def test_main_renders_text_report(self):
        dump_file = os.path.join(self.root, "dump.sql")
        with open(dump_file, "w", encoding="utf-8") as fp:
            fp.write(
                "INSERT INTO dashboard (id, uid, data) VALUES "
                "(1, 'abc123', '{\"title\":\"bad\",}');\n"
            )

        stdout = StringIO()
        with redirect_stdout(stdout):
            exit_code = main(["--dump-file", dump_file, "--tables", "dashboard"])

        output = stdout.getvalue()
        self.assertEqual(exit_code, 0)
        self.assertIn("invalid_json_payload", output)
        self.assertIn("column=data", output)


if __name__ == "__main__":
    unittest.main()
