import importlib
import sys
import types
from types import SimpleNamespace

import pytest


def _import_rag_module():
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
    setattr(langchain_core_pkg, "messages", lc_messages_mod)

    module = importlib.import_module("core.nodes.rag")
    return importlib.reload(module)


@pytest.fixture
def rag_module():
    return _import_rag_module()


def test_rag_node_loads_all_documents(monkeypatch: pytest.MonkeyPatch, rag_module) -> None:
    class FakeRepository:
        def get_all_documents(self):
            return [
                SimpleNamespace(page_content="Doc A", metadata={"source": "a"}),
                SimpleNamespace(page_content="Doc B", metadata={}),
            ]

    monkeypatch.setattr(rag_module, "get_repository", lambda: FakeRepository())

    state = {
        "messages": [],
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

    result = rag_module.rag_node(state)

    assert "Doc A" in result["retrieved_context"]
    assert "Doc B" in result["retrieved_context"]
    assert "metadata" in result["retrieved_context"]
