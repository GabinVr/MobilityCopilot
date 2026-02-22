from types import SimpleNamespace

import pytest


class FakeSystemMessage:
    def __init__(self, content: str):
        self.content = content
        self.type = "system"


class FakeHumanMessage:
    def __init__(self, content: str):
        self.content = content
        self.type = "human"


@pytest.fixture
def contradictor_module(core_node_importer):
    return core_node_importer(
        "core.nodes.contradictor",
        message_symbols={
            "SystemMessage": FakeSystemMessage,
            "HumanMessage": FakeHumanMessage,
        },
    )


def test_contradictor_node_returns_error_when_llm_missing(
    monkeypatch: pytest.MonkeyPatch, contradictor_module
) -> None:
    monkeypatch.setattr(contradictor_module, "get_llm", lambda: None)

    result = contradictor_module.contradictor_node({})

    assert result["contradictory_response"] is None
    assert result["error"] == "LLM is not configured."


def test_contradictor_node_generates_notes_from_state(
    monkeypatch: pytest.MonkeyPatch, contradictor_module
) -> None:
    captured = {}

    class FakeLLM:
        def invoke(self, payload):
            captured["payload"] = payload
            return SimpleNamespace(content="Data is partial; use caution.")

    monkeypatch.setattr(contradictor_module, "get_llm", lambda: FakeLLM())

    state = {
        "analytical_response": "Collision risk is low.",
        "query_results": "[(\"Montréal\", 12)]",
        "retrieved_context": "311 and collision glossary",
    }

    result = contradictor_module.contradictor_node(state)

    assert result["contradictor_notes"] == "Data is partial; use caution."
    assert isinstance(captured["payload"], list)
    assert len(captured["payload"]) == 2
    assert isinstance(captured["payload"][0], FakeSystemMessage)
    assert isinstance(captured["payload"][1], FakeHumanMessage)
    assert "Final answer: Collision risk is low." in captured["payload"][1].content
    assert "SQL results: [(\"Montréal\", 12)]" in captured["payload"][1].content
