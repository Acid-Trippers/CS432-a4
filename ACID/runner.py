"""
ACID Test Runner

Terminal usage:
    python -m ACID.runner --test atomicity
    python -m ACID.runner --test all
    python -m ACID.runner --test advanced

Dashboard usage:
    from ACID.runner import run_acid_test
    result = run_acid_test("atomicity")
"""

import sys
import json
import argparse
from ACID.validators import (
    atomicity_test,
    consistency_test,
    isolation_test,
    durability_test
)
from ACID.advanced_validators import (
    multi_record_atomicity_test,
    cross_db_atomicity_test,
    not_null_constraint_test,
    schema_validation_test,
    dirty_read_test,
    concurrent_read_write_isolation_test,
    persistent_connection_test,
    index_integrity_test
)


def run_acid_test(test_name: str):
    """Run a single ACID test. Dashboard-friendly function."""
    tests = {
        "atomicity": atomicity_test,
        "consistency": consistency_test,
        "isolation": isolation_test,
        "durability": durability_test
    }
    
    if test_name not in tests:
        return {"error": f"Unknown test: {test_name}"}
    
    try:
        result = tests[test_name]()
        return result
    except Exception as e:
        return {"test": test_name, "passed": False, "error": str(e)}


def run_advanced_test(test_name: str):
    """Run a single advanced ACID test."""
    tests = {
        "multi_record_atomicity": multi_record_atomicity_test,
        "cross_db_atomicity": cross_db_atomicity_test,
        "not_null_constraint": not_null_constraint_test,
        "schema_validation": schema_validation_test,
        "dirty_read_prevention": dirty_read_test,
        "concurrent_read_write_isolation": concurrent_read_write_isolation_test,
        "persistent_connection": persistent_connection_test,
        "index_integrity": index_integrity_test
    }
    
    if test_name not in tests:
        return {"error": f"Unknown test: {test_name}"}
    
    try:
        result = tests[test_name]()
        return result
    except Exception as e:
        return {"test": test_name, "passed": False, "error": str(e)}


def run_all_tests():
    """Run all ACID tests and return results."""
    results = {}
    for test in ["atomicity", "consistency", "isolation", "durability"]:
        results[test] = run_acid_test(test)
    return results


def run_all_advanced_tests():
    """Run all advanced ACID tests."""
    results = {}
    advanced_tests = ["multi_record_atomicity", "cross_db_atomicity", "not_null_constraint",
                      "schema_validation", "dirty_read_prevention", "concurrent_read_write_isolation",
                      "persistent_connection", "index_integrity"]
    for test in advanced_tests:
        results[test] = run_advanced_test(test)
    return results


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Run ACID validation tests"
    )
    parser.add_argument(
        "--test",
        default="all",
        help=(
            "Test to run: atomicity, consistency, isolation, durability, "
            "all, advanced, or advanced_<name>"
        ),
    )

    args = parser.parse_args()
    test_name = args.test
    
    if test_name == "all":
        results = run_all_tests()
    elif test_name == "advanced":
        results = run_all_advanced_tests()
    elif test_name.startswith("advanced_"):
        results = {test_name: run_advanced_test(test_name.replace("advanced_", ""))}
    else:
        results = {test_name: run_acid_test(test_name)}
    
    # Print results
    for test, result in results.items():
        passed = result.get("passed", False)
        status = "✓ PASS" if passed else "✗ FAIL"
        print(f"\n{test.upper():25} {status}")
        for key, value in result.items():
            if key not in ["test", "passed"]:
                print(f"  {key}: {value}")


if __name__ == "__main__":
    main()
