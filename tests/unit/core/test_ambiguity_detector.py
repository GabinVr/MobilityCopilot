from types import SimpleNamespace

import pytest


@pytest.fixture
def ambiguity_module(core_node_importer):
    return core_node_importer("core.nodes.ambiguity_detector")


def test_ambiguity_node_returns_structured_fields(
    monkeypatch: pytest.MonkeyPatch, ambiguity_module
) -> None:
    class FakeStructuredInvoker:
        def invoke(self, _prompt: str):
            return SimpleNamespace(
                is_ambiguous=True,
                clarification_options=["Option A", "Option B"],
                need_external_data=True,
                language="français",
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
                language="français",
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
