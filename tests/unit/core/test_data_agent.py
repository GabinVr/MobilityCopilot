import importlib
import sys
import types
from types import SimpleNamespace

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


def test_data_agent_node_binds_tools_and_returns_message(
    monkeypatch: pytest.MonkeyPatch, data_agent_module
) -> None:
    captured = {}

    class FakeLLM:
        def bind_tools(self, tools, **_kwargs):
            captured["tools"] = tools
            return self

        def invoke(self, payload):
            captured["payload"] = payload
            return SimpleNamespace(content="DATA GATHERING COMPLETE: []", tool_calls=[])

    monkeypatch.setattr(data_agent_module, "get_llm", lambda: FakeLLM())

    state = {
        "question": "Donne-moi un résumé des collisions",
        "messages": [SimpleNamespace(type="human", content="Question")],
    }

    result = data_agent_module.data_agent_node(state)

    assert "messages" in result
    assert result["messages"][0].content == "DATA GATHERING COMPLETE: []"
    assert len(captured["tools"]) == 4
    assert len(captured["payload"]) == 2


def test_data_agent_node_uses_default_question_when_missing(
    monkeypatch: pytest.MonkeyPatch, data_agent_module
) -> None:
    captured = {}

    class FakeLLM:
        def bind_tools(self, _tools, **_kwargs):
            return self

        def invoke(self, payload):
            captured["payload"] = payload
            return SimpleNamespace(content="ok", tool_calls=[])

    monkeypatch.setattr(data_agent_module, "get_llm", lambda: FakeLLM())

    state = {
        "messages": [SimpleNamespace(type="human", content="Analyse")],
    }

    data_agent_module.data_agent_node(state)

    system_prompt = captured["payload"][0].content
    assert "The question to answer is: No question found." in system_prompt


def test_data_agent_node_passes_existing_messages_to_llm(
    monkeypatch: pytest.MonkeyPatch, data_agent_module
) -> None:
    captured = {}

    class FakeLLM:
        def bind_tools(self, _tools, **_kwargs):
            return self

        def invoke(self, payload):
            captured["payload"] = payload
            return SimpleNamespace(content="ok", tool_calls=[])

    monkeypatch.setattr(data_agent_module, "get_llm", lambda: FakeLLM())

    user_msg = SimpleNamespace(type="human", content="Trouve les hotspots")
    state = {
        "question": "Trouve les hotspots",
        "messages": [user_msg],
    }

    data_agent_module.data_agent_node(state)

    assert captured["payload"][1] is user_msg
