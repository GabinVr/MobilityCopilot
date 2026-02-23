import importlib
import sys
import types
from types import SimpleNamespace
from typing import Any, Sequence

import pytest


def _import_data_agent_module():
    langgraph_pkg = sys.modules.setdefault("langgraph", types.ModuleType("langgraph"))
    graph_pkg = sys.modules.setdefault("langgraph.graph", types.ModuleType("langgraph.graph"))
    message_mod = sys.modules.setdefault(
        "langgraph.graph.message", types.ModuleType("langgraph.graph.message")
    )
    setattr(message_mod, "add_messages", lambda messages: messages)
    setattr(graph_pkg, "message", message_mod)
    setattr(langgraph_pkg, "graph", graph_pkg)

    langchain_core_pkg = sys.modules.setdefault(
        "langchain_core", types.ModuleType("langchain_core")
    )
    lc_messages_mod = sys.modules.setdefault(
        "langchain_core.messages", types.ModuleType("langchain_core.messages")
    )
    setattr(lc_messages_mod, "AnyMessage", object)
    setattr(lc_messages_mod, "SystemMessage", lambda content: SimpleNamespace(content=content))
    setattr(langchain_core_pkg, "messages", lc_messages_mod)

    llm_provider_mod = sys.modules.setdefault(
        "utils.llm_provider", types.ModuleType("utils.llm_provider")
    )
    setattr(llm_provider_mod, "get_llm", lambda: None)

    weather_tools_mod = sys.modules.setdefault(
        "core.tools.tools_api_weather_now", types.ModuleType("core.tools.tools_api_weather_now")
    )
    setattr(weather_tools_mod, "geomet_mtl_weather_text_bundle", object())

    histo_tools_mod = sys.modules.setdefault(
        "core.tools.tools_api_histo", types.ModuleType("core.tools.tools_api_histo")
    )
    setattr(histo_tools_mod, "geomet_mtl_history_global_tool", object())

    module = importlib.import_module("core.nodes.data_agent")
    return importlib.reload(module)


@pytest.fixture
def data_agent_module():
    return _import_data_agent_module()


def test_strip_llm_wrappers_removes_fences_and_tags(data_agent_module) -> None:
    raw = """
    <response>
    ```sql
    SELECT id, name FROM stops;
    ```
    </response>
    """

    assert data_agent_module._strip_llm_wrappers(raw) == "SELECT id, name FROM stops;"


def test_sanitize_sql_query_accepts_read_only_and_normalizes(data_agent_module) -> None:
    candidate = "SELECT  id  FROM stops -- comment\nWHERE city = 'Caen';"

    sanitized = data_agent_module._sanitize_sql_query(candidate)

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
    data_agent_module = _import_data_agent_module()

    with pytest.raises(ValueError, match=expected_message):
        data_agent_module._sanitize_sql_query(candidate)


def test_data_agent_node_returns_error_when_no_human_message(data_agent_module) -> None:
    state = {
        "messages": [SimpleNamespace(type="ai", content="Hello")],
        "audience": "grand_public",
        "is_ambiguous": False,
        "clarification_options": None,
        "retrieved_context": "",
        "generated_query": None,
        "query_results": None,
        "query_error": None,
        "analytical_response": None,
        "contradictor_notes": None,
    }

    result = data_agent_module.data_agent_node(state)

    assert result["generated_query"] is None
    assert "No user question found" in result["query_error"]


def test_data_agent_node_parses_and_sanitizes_llm_output(
    monkeypatch: pytest.MonkeyPatch, data_agent_module
) -> None:
    class FakeLLM:
        def bind_tools(self, _tools: Sequence[Any]):
            return self

        def invoke(self, _prompt: Sequence[Any]):
            return SimpleNamespace(content="<sql>```sql\nSELECT * FROM trips;\n```</sql>", tool_calls=[])

    monkeypatch.setattr(data_agent_module, "get_llm", lambda: FakeLLM())

    state = {
        "messages": [SimpleNamespace(type="human", content="Donne moi tous les trajets")],
        "audience": "grand_public",
        "is_ambiguous": False,
        "clarification_options": None,
        "retrieved_context": "table trips(id, route_name)",
        "generated_query": None,
        "query_results": None,
        "query_error": None,
        "analytical_response": None,
        "contradictor_notes": None,
    }

    result = data_agent_module.data_agent_node(state)

    assert result["query_error"] is None
    assert result["generated_query"] == "SELECT * FROM trips"


def test_data_agent_node_rejects_unsafe_llm_output(
    monkeypatch: pytest.MonkeyPatch, data_agent_module
) -> None:
    class FakeLLM:
        def bind_tools(self, _tools: Sequence[Any]):
            return self

        def invoke(self, _prompt: Sequence[Any]):
            return SimpleNamespace(content="```sql\nDELETE FROM trips\n```", tool_calls=[])

    monkeypatch.setattr(data_agent_module, "get_llm", lambda: FakeLLM())

    state = {
        "messages": [SimpleNamespace(type="human", content="Supprime les trajets")],
        "audience": "municipalite",
        "is_ambiguous": False,
        "clarification_options": None,
        "retrieved_context": "table trips(id, route_name)",
        "generated_query": None,
        "query_results": None,
        "query_error": None,
        "analytical_response": None,
        "contradictor_notes": None,
    }

    result = data_agent_module.data_agent_node(state)

    assert result["generated_query"] is None
    assert "Unsafe or invalid SQL generated" in result["query_error"]
