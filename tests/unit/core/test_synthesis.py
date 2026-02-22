import importlib
import sys
import types
from types import SimpleNamespace

import pytest


class FakeHumanMessage:
    def __init__(self, content: str):
        self.content = content
        self.type = "human"


class FakeSystemMessage:
    def __init__(self, content: str):
        self.content = content
        self.type = "system"


def _import_synthesis_module():
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
    setattr(lc_messages_mod, "SystemMessage", FakeSystemMessage)
    setattr(langchain_core_pkg, "messages", lc_messages_mod)

    llm_provider_mod = sys.modules.setdefault(
        "utils.llm_provider", types.ModuleType("utils.llm_provider")
    )
    setattr(llm_provider_mod, "get_llm", lambda: None)

    module = importlib.import_module("core.nodes.synthesis")
    return importlib.reload(module)


@pytest.fixture
def synthesis_module():
    return _import_synthesis_module()


def test_synthesis_node_builds_final_response(
    monkeypatch: pytest.MonkeyPatch, synthesis_module
) -> None:
    class FakeLLM:
        def __init__(self):
            self.last_input = None

        def invoke(self, payload):
            self.last_input = payload
            return SimpleNamespace(content="Voici la synthèse finale.", type="ai")

    fake_llm = FakeLLM()
    monkeypatch.setattr(synthesis_module, "get_llm", lambda: fake_llm)

    state = {
        "messages": [SimpleNamespace(type="human", content="Analyse les données")],
        "audience": "grand_public",
        "retrieved_context": "Contexte RAG",
        "sql_results": "[('A', 10)]",
    }

    result = synthesis_module.synthesis_node(state)

    assert result["analytical_response"] == "Voici la synthèse finale."
    assert result["messages"][0].content == "Voici la synthèse finale."
    assert isinstance(fake_llm.last_input, list)
    assert "STYLE GUIDE" in fake_llm.last_input[0].content


def test_synthesis_node_uses_specialized_style_for_municipalite(
    monkeypatch: pytest.MonkeyPatch, synthesis_module
) -> None:
    class FakeLLM:
        def __init__(self):
            self.last_prompt = ""

        def invoke(self, payload):
            self.last_prompt = payload[0].content
            return SimpleNamespace(content="Réponse technique", type="ai")

    fake_llm = FakeLLM()
    monkeypatch.setattr(synthesis_module, "get_llm", lambda: fake_llm)

    state = {
        "messages": [SimpleNamespace(type="human", content="Besoin de détails techniques")],
        "audience": "municipalite",
        "retrieved_context": "Contexte",
        "sql_results": "[]",
    }

    synthesis_module.synthesis_node(state)

    assert "technical depth" in fake_llm.last_prompt
