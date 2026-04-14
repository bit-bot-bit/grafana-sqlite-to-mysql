import os
import shutil
from contextlib import redirect_stdout
from io import StringIO
import unittest

from scan_sql_dump_risks import main, scan_dump


class ScanSqlDumpRisksTests(unittest.TestCase):
    def setUp(self):
        self.root = os.path.join(os.getcwd(), ".tmp_scan_risks")
        shutil.rmtree(self.root, ignore_errors=True)
        os.makedirs(self.root)

    def tearDown(self):
        shutil.rmtree(self.root, ignore_errors=True)

    def test_flags_even_backslashes_before_single_quote(self):
        dump_file = os.path.join(self.root, "dump.sql")
        with open(dump_file, "w", encoding="utf-8") as fp:
            fp.write(
                "INSERT OR REPLACE INTO dashboard VALUES "
                "(1, '{\"title\":\"bad\\\\\\\\'quote\"}');\n"
            )

        findings = scan_dump(dump_file, ())

        self.assertTrue(
            any(finding.code == "even_backslashes_before_single_quote" for finding in findings)
        )
        self.assertFalse(
            any(finding.code == "transform_payload_changed" for finding in findings)
        )

    def test_ignores_other_tables_when_filter_is_set(self):
        dump_file = os.path.join(self.root, "dump.sql")
        with open(dump_file, "w", encoding="utf-8") as fp:
            fp.write("INSERT OR REPLACE INTO alert_rule VALUES (1, 'bad\\\\\\\\'quote');\n")

        findings = scan_dump(dump_file, ("dashboard",))

        self.assertEqual(findings, [])

    def test_main_renders_text_report(self):
        dump_file = os.path.join(self.root, "dump.sql")
        with open(dump_file, "w", encoding="utf-8") as fp:
            fp.write(
                "INSERT OR REPLACE INTO dashboard VALUES "
                "(1, '{\"title\":\"bad\\\\\\\\'quote\"}');\n"
            )

        stdout = StringIO()
        with redirect_stdout(stdout):
            exit_code = main(["--dump-file", dump_file, "--tables", "dashboard"])

        output = stdout.getvalue()
        self.assertEqual(exit_code, 0)
        self.assertIn("Findings: 1 total", output)
        self.assertIn("even_backslashes_before_single_quote", output)


if __name__ == "__main__":
    unittest.main()
