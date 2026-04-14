import os
import shutil
import unittest
from contextlib import redirect_stdout
from io import StringIO

from scan_sql_dump_fk_order import analyze_dump, main


class ScanSqlDumpFkOrderTests(unittest.TestCase):
    def setUp(self):
        self.root = os.path.join(os.getcwd(), ".tmp_scan_fk_order")
        shutil.rmtree(self.root, ignore_errors=True)
        os.makedirs(self.root)

    def tearDown(self):
        shutil.rmtree(self.root, ignore_errors=True)

    def test_orders_explicit_and_inferred_dependencies(self):
        dump_file = os.path.join(self.root, "dump.sql")
        with open(dump_file, "w", encoding="utf-8") as fp:
            fp.write(
                "CREATE TABLE org (id BIGINT PRIMARY KEY);\n"
                "CREATE TABLE dashboard (id BIGINT PRIMARY KEY, org_id BIGINT, "
                "FOREIGN KEY (org_id) REFERENCES org(id));\n"
                "CREATE TABLE alert_rule (id BIGINT PRIMARY KEY, dashboard_id BIGINT);\n"
                "INSERT INTO org (id) VALUES (1);\n"
                "INSERT INTO dashboard (id, org_id) VALUES (10, 1);\n"
                "INSERT INTO alert_rule (id, dashboard_id) VALUES (20, 10);\n"
            )

        result = analyze_dump(dump_file, ())

        self.assertEqual(result["suggested_order"], ["org", "dashboard", "alert_rule"])
        edges = {(edge["parent"], edge["child"], edge["source"]) for edge in result["edges"]}
        self.assertIn(("org", "dashboard", "explicit"), edges)
        self.assertIn(("dashboard", "alert_rule", "inferred"), edges)

    def test_main_renders_text_report(self):
        dump_file = os.path.join(self.root, "dump.sql")
        with open(dump_file, "w", encoding="utf-8") as fp:
            fp.write(
                "CREATE TABLE org (id BIGINT PRIMARY KEY);\n"
                "CREATE TABLE team (id BIGINT PRIMARY KEY, org_id BIGINT);\n"
                "INSERT INTO org (id) VALUES (1);\n"
                "INSERT INTO team (id, org_id) VALUES (2, 1);\n"
            )

        stdout = StringIO()
        with redirect_stdout(stdout):
            exit_code = main(["--dump-file", dump_file])

        output = stdout.getvalue()
        self.assertEqual(exit_code, 0)
        self.assertIn("Suggested apply order:", output)
        self.assertIn("org -> team", output)


if __name__ == "__main__":
    unittest.main()
