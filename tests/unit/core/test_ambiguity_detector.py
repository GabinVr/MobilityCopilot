import importlib
import sys
import types
from types import SimpleNamespace

import pytest


def _import_ambiguity_module():
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

    llm_provider_mod = sys.modules.setdefault(
        "utils.llm_provider", types.ModuleType("utils.llm_provider")
    )
    setattr(llm_provider_mod, "get_llm", lambda: None)

    module = importlib.import_module("core.nodes.ambiguity_detector")
    return importlib.reload(module)


@pytest.fixture
def ambiguity_module():
    return _import_ambiguity_module()


def test_ambiguity_node_returns_structured_fields(
    monkeypatch: pytest.MonkeyPatch, ambiguity_module
) -> None:
    class FakeStructuredInvoker:
        def invoke(self, _prompt: str):
            return SimpleNamespace(
                is_ambiguous=True,
                clarification_options=["Option A", "Option B"],
                need_external_data=True,
            )

    class FakeLLM:
        def with_structured_output(self, _schema):
            return FakeStructuredInvoker()

    monkeypatch.setattr(ambiguity_module, "get_llm", lambda: FakeLLM())

    state = {
        "messages": [SimpleNamespace(type="human", content="Où sont les problèmes ?")],
        "audience": "grand_public",
        "is_ambiguous": False,
        "clarification_options": None,
        "need_external_data": False,
        "retrieved_context": "Dataset 311 + collisions glossary",
        "generated_query": None,
        "query_results": None,
        "query_error": None,
        "analytical_response": None,
        "contradictor_notes": None,
    }

    result = ambiguity_module.ambiguity_node(state)

    assert result["is_ambiguous"] is True
    assert result["clarification_options"] == ["Option A", "Option B"]
    assert result["need_external_data"] is True


def test_ambiguity_node_handles_empty_messages(
    monkeypatch: pytest.MonkeyPatch, ambiguity_module
) -> None:
    class FakeStructuredInvoker:
        def __init__(self):
            self.last_prompt = ""

        def invoke(self, prompt: str):
            self.last_prompt = prompt
            return SimpleNamespace(
                is_ambiguous=False,
                clarification_options=[],
                need_external_data=False,
            )

    invoker = FakeStructuredInvoker()

    class FakeLLM:
        def with_structured_output(self, _schema):
            return invoker

    monkeypatch.setattr(ambiguity_module, "get_llm", lambda: FakeLLM())

    state = {
        "messages": [],
        "audience": "grand_public",
        "is_ambiguous": False,
        "clarification_options": None,
        "need_external_data": False,
        "retrieved_context": "Glossary",
        "generated_query": None,
        "query_results": None,
        "query_error": None,
        "analytical_response": None,
        "contradictor_notes": None,
    }

    result = ambiguity_module.ambiguity_node(state)

    assert result["is_ambiguous"] is False
    assert "USER QUESTION:" in invoker.last_prompt
