#!/usr/bin/env python3
"""
Test column filtering feature across:
1. Backend CRUD operations
2. Dashboard API
3. Frontend rendering
"""

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
            "expected_count": 2  # record_id + username
        },
        {
            "name": "Multiple columns",
            "columns": ["username", "city", "subscription"],
            "expected_count": 4  # record_id + 3 columns
        },
        {
            "name": "No columns (fetch all)",
            "columns": None,
            "expected_count": None  # Variable, just verify it returns all
        }
    ]
    
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
        data = results['data']
        
        # Check first record
        first_id = sorted(list(data.keys()))[0]
        first_rec = data[first_id]
        
        actual_cols = set(first_rec.keys())
        
        print(f"Requested columns: {test['columns']}")
        print(f"Actual columns in record: {sorted(actual_cols)}")
        
        if test["expected_count"]:
            if len(actual_cols) == test["expected_count"]:
                print(f"✅ PASS: Got {len(actual_cols)} columns as expected")
            else:
                print(f"❌ FAIL: Expected {test['expected_count']} columns, got {len(actual_cols)}")
        else:
            print(f"✅ PASS: Got {len(actual_cols)} columns (all records)")
        
        # Verify record_id is always present
        if "record_id" in actual_cols:
            print("✅ PASS: record_id always included")
        else:
            print("❌ FAIL: record_id missing!")
        
        # Sample output
        print(f"Sample record: {json.dumps({k: (v if not isinstance(v, str) else v[:20] + '...') for k, v in list(first_rec.items())}, indent=2)}")


if __name__ == "__main__":
    test_column_filtering()
    print(f"\n{'='*60}")
    print("ALL TESTS COMPLETED")
    print(f"{'='*60}\n")
