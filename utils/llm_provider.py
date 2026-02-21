import logging
import os
from abc import ABC, abstractmethod
from typing import Any, List, Optional

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


def _parse_optional_int(value: Optional[str]) -> Optional[int]:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except ValueError as exc:
        raise ValueError(f"Expected integer value, got: {value!r}") from exc


def _parse_optional_float(value: Optional[str]) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except ValueError as exc:
        raise ValueError(f"Expected float value, got: {value!r}") from exc


def get_llm() -> Any:
    """Return an initialized LangChain chat model from the selected provider."""
    provider = os.getenv("LLM_PROVIDER", "ollama").lower()

    if provider in ("google", "gemini"):
        from langchain_google_genai import ChatGoogleGenerativeAI

        api_key = os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")
        if not api_key:
            raise ValueError("GEMINI_API_KEY (or GOOGLE_API_KEY) is required for Gemini")
        return ChatGoogleGenerativeAI(
            google_api_key=api_key,
            model=os.getenv("GEMINI_MODEL", "gemini-pro"),
            temperature=0,
        )

    if provider == "ollama":
        from langchain_community.chat_models import ChatOllama

        return ChatOllama(
            base_url=os.getenv("OLLAMA_SERVER", "http://localhost:11434"),
            model=os.getenv("OLLAMA_MODEL", "llama3.2:latest"),
            temperature=0,
        )

    if provider == "mistral":
        from langchain_mistralai.chat_models import ChatMistralAI

        api_key = os.getenv("MISTRAL_API_KEY")
        if not api_key:
            raise ValueError("MISTRAL_API_KEY is required for Mistral")
        return ChatMistralAI(
            api_key=api_key,
            model=os.getenv("MISTRAL_MODEL", "mistral-large-latest"),
            temperature=0,
        )

    if provider == "openai":
        from langchain_openai import ChatOpenAI

        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise ValueError("OPENAI_API_KEY is required for OpenAI")
        return ChatOpenAI(
            api_key=api_key,
            model=os.getenv("OPENAI_MODEL", "gpt-4"),
            temperature=0,
        )

    if provider in ("github_models", "github"):
        from langchain_openai import ChatOpenAI

        token = os.getenv("GITHUB_MODEL_TOKEN")
        if not token:
            raise ValueError("GITHUB_MODEL_TOKEN is required for GitHub Models (needs models:read)")

        # IMPORTANT: model IDs are like "openai/gpt-4.1" (publisher/model_name).
        # The Marketplace page slug may not be the exact ID; check the catalog if unsure.
        kwargs = {
            "api_key": token,
            "base_url": os.getenv("GITHUB_MODELS_BASE_URL", "https://models.github.ai/inference"),
            "model": os.getenv("GITHUB_MODEL", "openai/gpt-5"),
            "default_headers": {
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
            },
        }
        temperature = _parse_optional_float(os.getenv("GITHUB_MODEL_TEMPERATURE"))
        if temperature is not None:
            kwargs["temperature"] = temperature
        return ChatOpenAI(**kwargs)

    raise ValueError(
        f"Unsupported LLM provider: {provider}. Supported providers: "
        f"{LLMFactory.get_available_providers()}"
    )



class LLMProvider(ABC):
    """Abstract base class for LLM providers."""
    
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.llm: Optional[Any] = None
    
    @abstractmethod
    def initialize(self) -> Any:
        """Initialize the LLM client."""
        pass
    
    @abstractmethod
    def get_name(self) -> str:
        """Return the name of the provider."""
        pass
    
    def invoke(self, prompt: str, **kwargs) -> str:
        """Invoke the LLM with a prompt and return the response as text."""
        if not self.llm:
            self.llm = self.initialize()
        result = self.llm.invoke(prompt, **kwargs)
        return self._to_text(result)
    
    def stream(self, prompt: str, **kwargs):
        """Invoke the LLM in streaming mode, yielding chunks of text."""
        if not self.llm:
            self.llm = self.initialize()
        yield from self.llm.stream(prompt, **kwargs)
    
    def batch(self, prompts: List[str], **kwargs) -> List[str]:
        """Invoke the LLM with a batch of prompts and return a list of responses."""
        if not self.llm:
            self.llm = self.initialize()
        results = self.llm.batch(prompts, **kwargs)
        return [self._to_text(r) for r in results]

    @staticmethod
    def _to_text(result: Any) -> str:
        """Convert various LLM response formats to plain text."""
        if result is None:
            return ""
        if isinstance(result, str):
            return result

        content = getattr(result, "content", None)
        if isinstance(content, str):
            return content
        if isinstance(content, list):
            return "".join(
                item.get("text", "") if isinstance(item, dict) else str(item)
                for item in content
            )

        return str(result)


class OllamaProvider(LLMProvider):
    """Provider for local Ollama models."""
    
    def __init__(self, 
                 server_url: Optional[str] = None,
                 model: Optional[str] = None,
                 temperature: float = 0.7,
                 top_p: float = 0.9,
                 **kwargs):
        """Initialize the Ollama provider."""
        super().__init__(**kwargs)
        self.server_url = server_url or os.getenv("OLLAMA_SERVER", "http://localhost:11434")
        self.model = model or os.getenv("OLLAMA_MODEL", "llama3.2:latest")
        self.temperature = temperature
        self.top_p = top_p
    
    def initialize(self) -> Any:
        """Initialize the Ollama client."""
        try:
            from langchain_community.chat_models import ChatOllama
            
            self.llm = ChatOllama(
                base_url=self.server_url,
                model=self.model,
                temperature=self.temperature,
                top_p=self.top_p,
                **self.kwargs
            )
            logger.info("Initialized Ollama model '%s' at %s", self.model, self.server_url)
            return self.llm
        except ImportError:
            raise ImportError("Ollama provider requires: pip install langchain-community")
    
    def get_name(self) -> str:
        return f"Ollama ({self.model})"


class OpenAIProvider(LLMProvider):
    """Provider for OpenAI."""
    
    def __init__(self,
                 api_key: Optional[str] = None,
                 model: Optional[str] = None,
                 temperature: float = 0.7,
                 max_tokens: Optional[int] = None,
                 **kwargs):
        """Initialize the OpenAI provider."""
        super().__init__(**kwargs)
        self.api_key = api_key or os.getenv("OPENAI_API_KEY")
        self.model = model or os.getenv("OPENAI_MODEL", "gpt-4")
        self.temperature = temperature
        self.max_tokens = max_tokens if max_tokens is not None else _parse_optional_int(os.getenv("LLM_MAX_TOKENS"))
        
        if not self.api_key:
            raise ValueError("OPENAI_API_KEY is required for OpenAI")
    
    def initialize(self) -> Any:
        """Initialize the OpenAI client."""
        try:
            from langchain_openai import ChatOpenAI
            
            self.llm = ChatOpenAI(
                api_key=self.api_key,
                model=self.model,
                temperature=self.temperature,
                max_tokens=self.max_tokens,
                **self.kwargs
            )
            logger.info("Initialized OpenAI model '%s'", self.model)
            return self.llm
        except ImportError:
            raise ImportError("OpenAI provider requires: pip install langchain-openai")
    
    def get_name(self) -> str:
        return f"OpenAI ({self.model})"


class GitHubModelsProvider(LLMProvider):
    """Provider for GitHub Models (via GitHub AI Inference)."""

    def __init__(self,
                 token: Optional[str] = None,
                 model: Optional[str] = None,
                 base_url: Optional[str] = None,
                 temperature: Optional[float] = None,
                 max_tokens: Optional[int] = None,
                 **kwargs):
        super().__init__(**kwargs)
        self.token = token or os.getenv("GITHUB_MODEL_TOKEN")
        self.model = model or os.getenv("GITHUB_MODEL", "openai/gpt-5")
        self.base_url = base_url or os.getenv("GITHUB_MODELS_BASE_URL", "https://models.github.ai/inference")
        self.temperature = (
            temperature
            if temperature is not None
            else _parse_optional_float(os.getenv("GITHUB_MODEL_TEMPERATURE"))
        )
        self.max_tokens = max_tokens if max_tokens is not None else _parse_optional_int(os.getenv("LLM_MAX_TOKENS"))

        if not self.token:
            raise ValueError("GITHUB_MODEL_TOKEN is required for GitHub Models (needs models:read)")

    def initialize(self) -> Any:
        try:
            from langchain_openai import ChatOpenAI

            kwargs = {
                "api_key": self.token,
                "base_url": self.base_url,
                "model": self.model,
                "max_tokens": self.max_tokens,
                "default_headers": {
                    "Accept": "application/vnd.github+json",
                    "X-GitHub-Api-Version": "2022-11-28",
                },
                **self.kwargs,
            }
            if self.temperature is not None:
                kwargs["temperature"] = self.temperature

            self.llm = ChatOpenAI(**kwargs)
            logger.info("Initialized GitHub Models '%s'", self.model)
            return self.llm
        except ImportError:
            raise ImportError("GitHub Models provider requires: pip install langchain-openai")

    def get_name(self) -> str:
        return f"GitHub Models ({self.model})"


class MistralProvider(LLMProvider):
    """Provider for Mistral AI."""
    
    def __init__(self,
                 api_key: Optional[str] = None,
                 model: Optional[str] = None,
                 temperature: float = 0.7,
                 max_tokens: Optional[int] = None,
                 **kwargs):
        """Initialize the Mistral provider."""
        super().__init__(**kwargs)
        self.api_key = api_key or os.getenv("MISTRAL_API_KEY")
        self.model = model or os.getenv("MISTRAL_MODEL", "mistral-large-latest")
        self.temperature = temperature
        self.max_tokens = max_tokens if max_tokens is not None else _parse_optional_int(os.getenv("LLM_MAX_TOKENS"))
        
        if not self.api_key:
            raise ValueError("MISTRAL_API_KEY is required for Mistral")
    
    def initialize(self) -> Any:
        """Initialize the Mistral client."""
        try:
            from langchain_mistralai.chat_models import ChatMistralAI
            
            self.llm = ChatMistralAI(
                api_key=self.api_key,
                model=self.model,
                temperature=self.temperature,
                max_tokens=self.max_tokens,
                **self.kwargs
            )
            logger.info("Initialized Mistral model '%s'", self.model)
            return self.llm
        except ImportError:
            raise ImportError("Mistral provider requires: pip install langchain-mistralai")
    
    def get_name(self) -> str:
        return f"Mistral ({self.model})"


class GeminiProvider(LLMProvider):
    """Provider for Google Gemini."""
    
    def __init__(self,
                 api_key: Optional[str] = None,
                 model: Optional[str] = None,
                 temperature: float = 0.7,
                 max_output_tokens: Optional[int] = None,
                 **kwargs):
        """Initialize the Gemini provider."""
        super().__init__(**kwargs)
        self.api_key = api_key or os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")
        self.model = model or os.getenv("GEMINI_MODEL", "gemini-pro")
        self.temperature = temperature
        self.max_output_tokens = (
            max_output_tokens
            if max_output_tokens is not None
            else _parse_optional_int(os.getenv("LLM_MAX_TOKENS"))
        )
        
        if not self.api_key:
            raise ValueError("GEMINI_API_KEY (or GOOGLE_API_KEY) is required for Gemini")
    
    def initialize(self) -> Any:
        """Initialize the Gemini client."""
        try:
            from langchain_google_genai import ChatGoogleGenerativeAI
            
            self.llm = ChatGoogleGenerativeAI(
                google_api_key=self.api_key,
                model=self.model,
                temperature=self.temperature,
                max_output_tokens=self.max_output_tokens,
                **self.kwargs
            )
            logger.info("Initialized Gemini model '%s'", self.model)
            return self.llm
        except ImportError:
            raise ImportError("Gemini provider requires: pip install langchain-google-generativeai")
    
    def get_name(self) -> str:
        return f"Gemini ({self.model})"


class LLMFactory:
    """Factory to build LLM provider instances."""
    
    _providers = {
        "ollama": OllamaProvider,
        "openai": OpenAIProvider,
        "mistral": MistralProvider,
        "gemini": GeminiProvider,
        "github_models": GitHubModelsProvider,
        "github": GitHubModelsProvider,
    }
    
    @classmethod
    def register_provider(cls, name: str, provider_class: type):
        """Register a new provider class."""
        cls._providers[name.lower()] = provider_class
    
    @classmethod
    def create(cls, provider_name: Optional[str] = None, **kwargs) -> LLMProvider:
        """Create a provider instance from its name or from `LLM_PROVIDER`."""
        provider_name = (provider_name or os.getenv("LLM_PROVIDER", "ollama")).lower()
        
        if provider_name not in cls._providers:
            available = list(cls._providers.keys())
            raise ValueError(
                f"Unknown provider '{provider_name}'. Available: {available}"
            )
        
        provider_class = cls._providers[provider_name]
        return provider_class(**kwargs)
    
    @classmethod
    def get_available_providers(cls) -> List[str]:
        """Return available provider names."""
        return list(cls._providers.keys())


class LLMManager:
    """Unified manager for provider selection and invocation."""
    
    def __init__(self, provider_name: Optional[str] = None, **kwargs):
        """Initialize the manager with the selected provider."""
        self.provider = LLMFactory.create(provider_name, **kwargs)
        self.llm = self.provider.initialize()
    
    def generate(self, prompt: str, **kwargs) -> str:
        """Generate a response for one prompt."""
        return self.provider.invoke(prompt, **kwargs)
    
    def stream_response(self, prompt: str, **kwargs):
        """Stream a response."""
        return self.provider.stream(prompt, **kwargs)
    
    def batch_generate(self, prompts: List[str], **kwargs) -> List[str]:
        """Generate responses for multiple prompts."""
        return self.provider.batch(prompts, **kwargs)
    
    def get_provider_name(self) -> str:
        """Return active provider name."""
        return self.provider.get_name()
    
    @staticmethod
    def list_providers() -> List[str]:
        """List available providers."""
        return LLMFactory.get_available_providers()


def get_llm_manager(provider: Optional[str] = None, **kwargs) -> LLMManager:
    """Convenience function to create an `LLMManager`."""
    return LLMManager(provider, **kwargs)


def quick_generate(prompt: str, provider: Optional[str] = None, **kwargs) -> str:
    """Convenience helper to generate one response quickly."""
    manager = LLMManager(provider, **kwargs)
    return manager.generate(prompt)


if __name__ == "__main__":
    print("Available LLM providers:", LLMFactory.get_available_providers())
