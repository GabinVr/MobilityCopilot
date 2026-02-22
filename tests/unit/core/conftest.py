import importlib
import sys
import types
from typing import Any, Callable

import pytest


@pytest.fixture
def core_node_importer() -> Callable[..., Any]:
    def _import_core_node_module(
        module_path: str,
        *,
        message_symbols: dict[str, Any] | None = None,
        extra_modules: dict[str, dict[str, Any]] | None = None,
    ) -> Any:
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

        for symbol_name, symbol_value in (message_symbols or {}).items():
            setattr(lc_messages_mod, symbol_name, symbol_value)

        setattr(langchain_core_pkg, "messages", lc_messages_mod)

        llm_provider_mod = sys.modules.setdefault(
            "utils.llm_provider", types.ModuleType("utils.llm_provider")
        )
        setattr(llm_provider_mod, "get_llm", lambda: None)

        for extra_module_name, attributes in (extra_modules or {}).items():
            extra_mod = sys.modules.setdefault(extra_module_name, types.ModuleType(extra_module_name))
            for attr_name, attr_value in attributes.items():
                setattr(extra_mod, attr_name, attr_value)

        module = importlib.import_module(module_path)
        return importlib.reload(module)

    return _import_core_node_module
