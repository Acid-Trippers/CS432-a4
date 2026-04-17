#!/usr/bin/env python3
"""
Test column filtering feature across:
1. Backend CRUD operations
2. Dashboard API
3. Frontend rendering
"""

import sys
import json
from src.config import QUERY_FILE
from src.phase_6.CRUD_runner import query_parser, analyze_query_databases
from src.phase_6.CRUD_operations import read_operation, refresh_connections


def test_column_filtering():
    """Test that column filtering works at the backend level"""
    refresh_connections()

    test_cases = [
        {
            "name": "Single column",
            "columns": ["username"],
        },
        {
            "name": "Multiple columns",
            "columns": ["username", "city", "subscription"],
        },
        {
            "name": "No columns (fetch all)",
            "columns": None,
        }
    ]

    failures = 0

    for test in test_cases:
        print(f"\n{'='*60}")
        print(f"TEST: {test['name']}")
        print(f"{'='*60}")

        query = {
            "operation": "READ",
            "entity": "main_records",
            "filters": {},
        }
        if test["columns"]:
            query["columns"] = test["columns"]
        
        # Write query
        with open(QUERY_FILE, 'w') as f:
            json.dump(query, f, indent=2)
        
        # Parse and execute
        parsed = query_parser()
        db_analysis = analyze_query_databases(parsed)
        results = read_operation(parsed, db_analysis)
        data = results.get("data", {})

        if not data:
            failures += 1
            print("FAIL: Query returned no rows.")
            print("  This usually means the environment is not initialized or source data is missing.")
            continue

        # Check first record
        first_id = sorted(list(data.keys()))[0]
        first_rec = data[first_id]

        actual_cols = set(first_rec.keys())

        print(f"Requested columns: {test['columns']}")
        print(f"Actual columns in record: {sorted(actual_cols)}")

        if test["columns"] is not None:
            expected_cols = {"record_id", *test["columns"]}
            if actual_cols == expected_cols:
                print(f"PASS: Exact projected columns returned ({len(actual_cols)} total)")
            else:
                failures += 1
                missing = sorted(expected_cols - actual_cols)
                extra = sorted(actual_cols - expected_cols)
                print("FAIL: Projected columns mismatch")
                print(f"  Missing: {missing if missing else 'None'}")
                print(f"  Extra:   {extra if extra else 'None'}")
        else:
            print(f"PASS: Returned full record view with {len(actual_cols)} columns")

        # Verify record_id is always present
        if "record_id" in actual_cols:
            print("PASS: record_id is included")
        else:
            failures += 1
            print("FAIL: record_id missing")

        # Sample output
        sample = {
            k: (v if not isinstance(v, str) else v[:20] + '...')
            for k, v in list(first_rec.items())
        }
        print(f"Sample record: {json.dumps(sample, indent=2)}")

    return failures


if __name__ == "__main__":
    failure_count = test_column_filtering()
    print(f"\n{'='*60}")
    if failure_count == 0:
        print("ALL TESTS PASSED")
    else:
        print(f"TESTS COMPLETED WITH {failure_count} FAILURE(S)")
    print(f"{'='*60}\n")
    sys.exit(0 if failure_count == 0 else 1)
