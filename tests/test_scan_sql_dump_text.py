import os
import shutil
import unittest
from contextlib import redirect_stdout
from io import StringIO

from scan_sql_dump_text import main, scan_dump


class ScanSqlDumpTextTests(unittest.TestCase):
    def setUp(self):
        self.root = os.path.join(os.getcwd(), ".tmp_scan_text")
        shutil.rmtree(self.root, ignore_errors=True)
        os.makedirs(self.root)

    def tearDown(self):
        shutil.rmtree(self.root, ignore_errors=True)

    def test_flags_replacement_character(self):
        dump_file = os.path.join(self.root, "dump.sql")
        with open(dump_file, "w", encoding="utf-8") as fp:
            fp.write("INSERT INTO dashboard (id, title) VALUES (1, 'bad � title');\n")

        findings = scan_dump(dump_file, (), set())

        self.assertEqual(len(findings), 1)
        self.assertEqual(findings[0].code, "replacement_character")

    def test_flags_possible_mojibake(self):
        dump_file = os.path.join(self.root, "dump.sql")
        with open(dump_file, "w", encoding="utf-8") as fp:
            fp.write("INSERT INTO dashboard (id, title) VALUES (1, 'FranÃ§ais');\n")

        findings = scan_dump(dump_file, (), {"title"})

        self.assertEqual(len(findings), 1)
        self.assertEqual(findings[0].code, "possible_mojibake")

    def test_main_renders_text_report(self):
        dump_file = os.path.join(self.root, "dump.sql")
        with open(dump_file, "w", encoding="utf-8") as fp:
            fp.write("INSERT INTO dashboard (id, title) VALUES (1, 'bad � title');\n")

        stdout = StringIO()
        with redirect_stdout(stdout):
            exit_code = main(["--dump-file", dump_file, "--tables", "dashboard"])

        output = stdout.getvalue()
        self.assertEqual(exit_code, 0)
        self.assertIn("replacement_character", output)
        self.assertIn("column=title", output)


if __name__ == "__main__":
    unittest.main()
