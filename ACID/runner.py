"""
ACID Test Runner

Terminal usage:
    python -m ACID.runner --test atomicity
    python -m ACID.runner --test all
    python -m ACID.runner --test advanced

Dashboard usage:
    from ACID.runner import run_acid_test, run_advanced_test, run_all_tests, run_all_advanced_tests
    result = run_acid_test("atomicity")
    result = run_advanced_test("concurrent_insert_lost_updates")
    all_results = run_all_advanced_tests()
"""

import argparse
from ACID.validators import (
    atomicity_test,
    multi_record_atomicity_test,
    consistency_test,
    isolation_test,
    durability_test
)
from ACID.advanced_validators import (
    not_null_constraint_test,
    schema_validation_test,
    dirty_read_test,
    concurrent_read_write_isolation_test,
    concurrent_insert_lost_updates_test,
    concurrent_update_atomicity_test,
    stress_test_concurrent_ops,
    persistent_connection_test,
    index_integrity_test
)


# Unified test registry (single source of truth for all tests)
TEST_REGISTRY = {
    # Core ACID tests
    "atomicity": atomicity_test,
    "multi_record_atomicity": multi_record_atomicity_test,
    "consistency": consistency_test,
    "isolation": isolation_test,
    "durability": durability_test,

    # Remaining advanced ACID tests
    "not_null_constraint": not_null_constraint_test,
    "schema_validation": schema_validation_test,
    "dirty_read_prevention": dirty_read_test,
    "concurrent_read_write_isolation": concurrent_read_write_isolation_test,
    "concurrent_insert_lost_updates": concurrent_insert_lost_updates_test,
    "concurrent_update_atomicity": concurrent_update_atomicity_test,
    "stress_test_concurrent_ops": stress_test_concurrent_ops,
    "persistent_connection": persistent_connection_test,
    "index_integrity": index_integrity_test,
}

# Grouping for convenience (used by CLI and bulk runners)
CORE_TESTS = [
    "atomicity",
    "multi_record_atomicity",
    "consistency",
    "isolation",
    "durability",
]

ADVANCED_TESTS = [
    "multi_record_atomicity",
    "not_null_constraint", "schema_validation",
    "dirty_read_prevention", "concurrent_read_write_isolation",
    "concurrent_insert_lost_updates", "concurrent_update_atomicity",
    "stress_test_concurrent_ops", "persistent_connection",
    "index_integrity"
]


def _run_test(test_name: str):
    """
    Internal helper to execute a single test.
    
    Args:
        test_name: Name of test from TEST_REGISTRY
        
    Returns:
        dict: Test result with 'passed' status and metrics
    """
    if test_name not in TEST_REGISTRY:
        return {"error": f"Unknown test: {test_name}"}
    
    try:
        result = TEST_REGISTRY[test_name]()
        return result
    except Exception as e:
        return {"test": test_name, "passed": False, "error": str(e)}


def run_acid_test(test_name: str):
    """Run a single core ACID test. Dashboard-friendly function."""
    return _run_test(test_name)


def run_advanced_test(test_name: str):
    """Run a single advanced ACID test. Dashboard-friendly function."""
    return _run_test(test_name)


def run_all_tests():
    """Run all core ACID tests and return results."""
    results = {}
    for test in CORE_TESTS:
        results[test] = _run_test(test)
    return results


def run_all_advanced_tests():
    """Run all advanced ACID tests."""
    results = {}
    for test in ADVANCED_TESTS:
        results[test] = _run_test(test)
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
            "all, advanced, or any test name"
        ),
    )

    args = parser.parse_args()
    test_name = args.test
    
    # Determine which tests to run
    if test_name == "all":
        results = run_all_tests()
    elif test_name == "advanced":
        results = run_all_advanced_tests()
    elif test_name.startswith("advanced_"):
        # Support legacy "advanced_" prefix for backward compatibility
        actual_test_name = test_name.replace("advanced_", "")
        results = {actual_test_name: _run_test(actual_test_name)}
    else:
        results = {test_name: _run_test(test_name)}
    
    # Print results
    for test, result in results.items():
        passed = result.get("passed", False)
        status = "✓ PASS" if passed else "✗ FAIL"
        print(f"\n{test.upper():25} {status}")
        for key, value in result.items():
            # Skip internal fields and error field for passing tests
            if key in ["test", "passed"]:
                continue
            # Only show error for failed tests
            if key == "error" and passed:
                continue
            print(f"  {key}: {value}")


if __name__ == "__main__":
    main()