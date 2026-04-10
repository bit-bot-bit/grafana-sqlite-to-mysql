import json
import os
import shutil
import subprocess
import sys
import unittest


class PerfFixtureGeneratorTests(unittest.TestCase):
    def setUp(self):
        self.root = os.path.join(os.getcwd(), ".tmp_perf_fixture")
        shutil.rmtree(self.root, ignore_errors=True)
        os.makedirs(self.root)

    def tearDown(self):
        shutil.rmtree(self.root, ignore_errors=True)

    def test_generator_writes_schema_dump_and_manifest(self):
        cmd = [
            sys.executable,
            "generate_perf_fixture.py",
            "--output-dir",
            self.root,
            "--target-size-mib",
            "1",
            "--rows-per-insert",
            "4",
        ]
        subprocess.run(cmd, check=True, cwd=os.getcwd())

        schema_path = os.path.join(self.root, "schema.sql")
        dump_path = os.path.join(self.root, "dump.sql")
        manifest_path = os.path.join(self.root, "manifest.json")

        self.assertTrue(os.path.exists(schema_path))
        self.assertTrue(os.path.exists(dump_path))
        self.assertTrue(os.path.exists(manifest_path))
        self.assertGreater(os.path.getsize(dump_path), 1024 * 1024)

        with open(manifest_path, "r", encoding="utf-8") as fp:
            manifest = json.load(fp)
        self.assertEqual(manifest["rows_per_insert"], 4)
        self.assertGreater(manifest["statements"], 0)
        self.assertIn("dashboard", manifest["tables"])


if __name__ == "__main__":
    unittest.main()
