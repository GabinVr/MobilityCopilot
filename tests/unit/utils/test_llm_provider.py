import importlib
import sys
import types

import pytest


class FakeChatOpenAI:
    last_kwargs = None

    def __init__(self, **kwargs):
        FakeChatOpenAI.last_kwargs = kwargs


def _import_llm_provider_module():
    langchain_openai_mod = sys.modules.setdefault(
        "langchain_openai", types.ModuleType("langchain_openai")
    )
    setattr(langchain_openai_mod, "ChatOpenAI", FakeChatOpenAI)

    module = importlib.import_module("utils.llm_provider")
    return importlib.reload(module)


@pytest.fixture
def llm_provider_module():
    return _import_llm_provider_module()


def test_github_models_provider_omits_temperature_by_default(
    monkeypatch: pytest.MonkeyPatch, llm_provider_module
) -> None:
    monkeypatch.setenv("GITHUB_MODEL_TOKEN", "token")
    monkeypatch.delenv("GITHUB_MODEL_TEMPERATURE", raising=False)

    provider = llm_provider_module.GitHubModelsProvider()
    provider.initialize()

    assert "temperature" not in FakeChatOpenAI.last_kwargs


def test_github_models_provider_passes_temperature_when_set(
    monkeypatch: pytest.MonkeyPatch, llm_provider_module
) -> None:
    monkeypatch.setenv("GITHUB_MODEL_TOKEN", "token")
    monkeypatch.setenv("GITHUB_MODEL_TEMPERATURE", "0.3")

    provider = llm_provider_module.GitHubModelsProvider()
    provider.initialize()

    assert FakeChatOpenAI.last_kwargs["temperature"] == 0.3


def test_get_llm_github_models_defaults(monkeypatch: pytest.MonkeyPatch, llm_provider_module) -> None:
    monkeypatch.setenv("LLM_PROVIDER", "github")
    monkeypatch.setenv("GITHUB_MODEL_TOKEN", "token")
    monkeypatch.delenv("GITHUB_MODEL", raising=False)
    monkeypatch.delenv("GITHUB_MODEL_TEMPERATURE", raising=False)

    llm_provider_module.get_llm()

    assert FakeChatOpenAI.last_kwargs["model"] == "openai/gpt-5"
    assert FakeChatOpenAI.last_kwargs["base_url"] == "https://models.github.ai/inference"
    assert "temperature" not in FakeChatOpenAI.last_kwargs
    assert FakeChatOpenAI.last_kwargs["default_headers"]["X-GitHub-Api-Version"] == "2022-11-28"
