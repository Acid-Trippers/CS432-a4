from fastapi import APIRouter, Depends, HTTPException, Query

from dashboard.dependencies import require_admin


router = APIRouter(prefix="/api/acid")


@router.get("/all")
async def run_all_acid_tests(
    _: str = Depends(require_admin),
):
    try:
        from ACID.runner import run_all_tests

        return run_all_tests()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@router.get("/advanced/{test_name}")
async def run_single_advanced_test(
    test_name: str,
    _: str = Depends(require_admin),
):
    try:
        from ACID.runner import run_advanced_test

        return run_advanced_test(test_name)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@router.get("/{test_name}")
async def run_single_acid_test(
    test_name: str,
    crash_check: bool = Query(False),
    _: str = Depends(require_admin),
):
    try:
        from ACID.runner import run_acid_test

        if test_name == "durability":
            return run_acid_test(test_name, crash_check=crash_check)
        return run_acid_test(test_name)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
