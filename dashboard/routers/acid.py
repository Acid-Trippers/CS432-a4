from fastapi import APIRouter, Depends, HTTPException, Query, Response

from dashboard.dependencies import get_session_id


router = APIRouter(prefix="/api/acid")


@router.get("/all")
async def run_all_acid_tests(
    response: Response,
    session_id: str = Depends(get_session_id),
):
    response.headers["X-Session-ID"] = session_id
    try:
        from ACID.runner import run_all_tests

        return run_all_tests()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@router.get("/advanced/{test_name}")
async def run_single_advanced_test(
    test_name: str,
    response: Response,
    session_id: str = Depends(get_session_id),
):
    response.headers["X-Session-ID"] = session_id
    try:
        from ACID.runner import run_advanced_test

        return run_advanced_test(test_name)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@router.get("/{test_name}")
async def run_single_acid_test(
    test_name: str,
    response: Response,
    crash_check: bool = Query(False),
    session_id: str = Depends(get_session_id),
):
    response.headers["X-Session-ID"] = session_id
    try:
        from ACID.runner import run_acid_test

        if test_name == "durability":
            return run_acid_test(test_name, crash_check=crash_check)
        return run_acid_test(test_name)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
