from typing import (
    Optional,
)

from .postgresql import (
    query_execute,
    query_execute_row,
    query_insert_from_csv,
    query_select,
    query_select_one,
)


def query_select_check(
    statement: str, parameters: Optional[dict] = None
) -> bool:
    """Enhance query function to get `check_exists` value from result."""
    return eval(
        query_select_one(statement, parameters=parameters)["check_exists"]
    )


def query_select_row(statement: str, parameters: Optional[dict] = None) -> int:
    """Enhance query function to get `row_number` value from result."""
    if any(
        _ in statement
        for _ in {
            "select count(*) as row_number from ",
            "func_count_if_exists",
        }
    ):
        return int(
            query_select_one(statement, parameters=parameters)["row_number"]
        )
    return query_execute_row(statement, parameters=parameters)
