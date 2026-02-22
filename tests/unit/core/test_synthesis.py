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

@pytest.fixture
def synthesis_module(core_node_importer):
    return core_node_importer(
        "core.nodes.synthesis",
        message_symbols={
            "HumanMessage": FakeHumanMessage,
            "SystemMessage": FakeSystemMessage,
        },
    )


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
        "audience": "grand public",
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
