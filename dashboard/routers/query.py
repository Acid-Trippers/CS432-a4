from typing import Any, Dict

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field, model_validator


router = APIRouter()


class QueryPayload(BaseModel):
    operation: str = Field(min_length=1)
    entity: str = Field(min_length=1)
    filters: Dict[str, Any] | None = None
    payload: Dict[str, Any] | None = None
    columns: list | None = None  # Column selection support

    @model_validator(mode="after")
    def validate_operation_requirements(self):
        valid_operations = {"CREATE", "READ", "UPDATE", "DELETE"}
        if self.operation not in valid_operations:
            raise ValueError(
                f"Invalid operation '{self.operation}'. Must be one of: {sorted(valid_operations)}"
            )

        if self.operation == "CREATE":
            if self.payload is None:
                raise ValueError("CREATE operation requires 'payload' field.")
            if not isinstance(self.payload, dict) or not self.payload:
                raise ValueError("Field 'payload' must be a non-empty object.")

        elif self.operation == "READ":
            if self.filters is None:
                raise ValueError("READ operation requires 'filters' field.")
            if not isinstance(self.filters, dict):
                raise ValueError("Field 'filters' must be a dict.")

        elif self.operation == "UPDATE":
            if self.filters is None:
                raise ValueError("UPDATE operation requires 'filters' field.")
            if not isinstance(self.filters, dict) or not self.filters:
                raise ValueError("Field 'filters' must be a non-empty object.")

            if self.payload is None:
                raise ValueError("UPDATE operation requires 'payload' field.")
            if not isinstance(self.payload, dict) or not self.payload:
                raise ValueError("Field 'payload' must be a non-empty object.")

        elif self.operation == "DELETE":
            if self.filters is None:
                raise ValueError("DELETE operation requires 'filters' field.")
            if not isinstance(self.filters, dict):
                raise ValueError("Field 'filters' must be a dict.")

        return self


@router.post("/api/query")
async def execute_query(body: QueryPayload):
    try:
        from src.phase_6.CRUD_operations import refresh_connections

        refresh_connections()
        from src.phase_6.CRUD_runner import query_runner

        result = query_runner(query_dict=body.model_dump())
        if result is None:
            raise HTTPException(status_code=400, detail="query could not be executed")
        return result
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
