# Test Report

Date: 2026-04-09

Command run:

```bash
python3 -m unittest tests.test_importer tests.test_parser
```

Result:

- Passed: 17 tests
- Failed: 0
- Errors: 0

Summary:

- Importer regression tests passed.
- Parser tests passed.
- Recent importer fixes remain covered, including:
  - final batch replay prevention
  - linear resume offset handling
  - parallel worker failure propagation
  - parallel table priority behavior

Observed stderr during the run:

- Environment noise about `dconf` and system bus access
- A Node/Angular CLI `ERR_REQUIRE_ESM` message from the local shell environment
- `ERROR:root:Worker 1 failed: boom`

Notes:

- The `Worker 1 failed: boom` log line is expected from a test that verifies worker failure handling.
- The environment warnings did not cause test failures.
