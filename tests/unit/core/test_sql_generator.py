import pytest

from core.tools import sql_generator as sql_mod


def test_strip_llm_wrappers_removes_fences_and_tags() -> None:
    raw = """
    <response>
    ```sql
    SELECT id, name FROM stops;
    ```
    </response>
    """

    assert sql_mod._strip_llm_wrappers(raw) == "SELECT id, name FROM stops;"


def test_sanitize_sql_query_accepts_read_only_and_normalizes() -> None:
    candidate = "SELECT  id  FROM stops -- comment\nWHERE city = 'Caen';"

    sanitized = sql_mod._sanitize_sql_query(candidate)

    assert sanitized == "SELECT id FROM stops WHERE city = 'Caen'"


@pytest.mark.parametrize(
    "candidate, expected_message",
    [
        ("DROP TABLE stops", "Only read-only SQL queries"),
        ("SELECT * FROM a; SELECT * FROM b", "Only one SQL statement is allowed"),
        ("WITH x AS (DELETE FROM t) SELECT * FROM x", "Forbidden SQL keyword"),
    ],
)
def test_sanitize_sql_query_rejects_unsafe_queries(candidate: str, expected_message: str) -> None:
    with pytest.raises(ValueError, match=expected_message):
        sql_mod._sanitize_sql_query(candidate)


def test_sql_generator_tool_returns_valid_sql_ready_to_execute() -> None:
    payload = "<sql>```sql\nSELECT * FROM trips;\n```</sql>"

    if hasattr(sql_mod.sql_generator_tool, "invoke"):
        result = sql_mod.sql_generator_tool.invoke({"query": payload})
    else:
        result = sql_mod.sql_generator_tool(payload)

    assert result == "VALID_SQL_READY_TO_EXECUTE: SELECT * FROM trips"


def test_sql_generator_tool_returns_sql_error_for_unsafe_sql() -> None:
    payload = "```sql\nDELETE FROM trips\n```"
    if hasattr(sql_mod.sql_generator_tool, "invoke"):
        result = sql_mod.sql_generator_tool.invoke({"query": payload})
    else:
        result = sql_mod.sql_generator_tool(payload)

    assert result.startswith("SQL_ERROR: Unsafe or invalid SQL generated:")
