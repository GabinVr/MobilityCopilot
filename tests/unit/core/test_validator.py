import importlib
import sys
import types
from types import SimpleNamespace

import pytest


class FakeHumanMessage:
    def __init__(self, content: str):
        self.content = content
        self.type = "human"


def _import_validator_module():
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
    setattr(lc_messages_mod, "HumanMessage", FakeHumanMessage)
    setattr(langchain_core_pkg, "messages", lc_messages_mod)

    lc_utils_mod = sys.modules.setdefault(
        "langchain_community.utilities", types.ModuleType("langchain_community.utilities")
    )
    setattr(lc_utils_mod, "SQLDatabase", object)

    module = importlib.import_module("core.nodes.validator")
    return importlib.reload(module)


@pytest.fixture
def validator_module():
    return _import_validator_module()


def test_execute_sql_node_returns_error_when_query_missing(validator_module) -> None:
    state = {"generated_query": None}

    result = validator_module.execute_sql_node(state)

    assert result["query_error"] == "Requête vide ou non générée."


def test_execute_sql_node_runs_query_successfully(
    monkeypatch: pytest.MonkeyPatch, validator_module
) -> None:
    called = {}

    class FakeDB:
        def run(self, query: str):
            called["query"] = query
            return [("ok", 1)]

    class FakeSQLDatabase:
        @staticmethod
        def from_uri(uri: str):
            called["uri"] = uri
            return FakeDB()

    monkeypatch.setattr(validator_module, "SQLDatabase", FakeSQLDatabase)

    result = validator_module.execute_sql_node({"generated_query": "SELECT 1"})

    assert result["query_results"] == "[('ok', 1)]"
    assert result["query_error"] is None
    assert called["query"] == "SELECT 1"
    assert called["uri"].startswith("sqlite:///")
    assert called["uri"].endswith("/data/db/mobility.db")


def test_execute_sql_node_returns_feedback_message_on_exception(
    monkeypatch: pytest.MonkeyPatch, validator_module
) -> None:
    class FakeSQLDatabase:
        @staticmethod
        def from_uri(_uri: str):
            raise RuntimeError("database unavailable")

    monkeypatch.setattr(validator_module, "SQLDatabase", FakeSQLDatabase)

    result = validator_module.execute_sql_node({"generated_query": "SELECT * FROM trips"})

    assert result["query_error"] == "database unavailable"
    assert isinstance(result["messages"], FakeHumanMessage)
    assert "SQL query failed" in result["messages"].content
