import importlib
import sys
import types
from types import SimpleNamespace

import pytest


class FakeCollection:
    def __init__(self, name: str):
        self.name = name

    def add(self, **_kwargs):
        return None


class FakeHttpClient:
    def __init__(self, host=None, port=None):
        self.host = host
        self.port = port
        self._collections = {}

    def list_collections(self):
        return [SimpleNamespace(name=name) for name in self._collections]

    def get_or_create_collection(self, name: str):
        if name not in self._collections:
            self._collections[name] = FakeCollection(name)
        return self._collections[name]

    def get_collection(self, name: str):
        return self.get_or_create_collection(name)

    def delete_collection(self, name: str):
        self._collections.pop(name, None)


class FakeChroma:
    last_call = None

    @classmethod
    def from_documents(cls, documents, embedding, client, collection_name):
        cls.last_call = {
            "documents": documents,
            "embedding": embedding,
            "client": client,
            "collection_name": collection_name,
        }


class FakeDocument:
    def __init__(self, page_content: str, metadata=None) -> None:
        self.page_content = page_content
        self.metadata = metadata or {}


def _import_chroma_client_module():
    chromadb_mod = sys.modules.setdefault("chromadb", types.ModuleType("chromadb"))
    setattr(chromadb_mod, "HttpClient", FakeHttpClient)

    langchain_chroma_mod = sys.modules.setdefault(
        "langchain_chroma", types.ModuleType("langchain_chroma")
    )
    setattr(langchain_chroma_mod, "Chroma", FakeChroma)

    lc_docs_mod = sys.modules.setdefault(
        "langchain_core.documents", types.ModuleType("langchain_core.documents")
    )
    setattr(lc_docs_mod, "Document", FakeDocument)

    module = importlib.import_module("utils.chroma_client")
    return importlib.reload(module)


@pytest.fixture
def chroma_client_module():
    return _import_chroma_client_module()


def test_chroma_client_uses_url(monkeypatch: pytest.MonkeyPatch, chroma_client_module) -> None:
    monkeypatch.delenv("CHROMA_URL", raising=False)

    client = chroma_client_module.ChromaClient(chroma_url="http://example.com:9999")

    assert client.host == "example.com"
    assert client.port == 9999


def test_chroma_client_uses_host_and_port(chroma_client_module) -> None:
    client = chroma_client_module.ChromaClient(host="127.0.0.1", port=1234)

    assert client.host == "127.0.0.1"
    assert client.port == 1234


def test_from_documents_forwards_to_chroma(chroma_client_module) -> None:
    docs = [FakeDocument("Doc A"), FakeDocument("Doc B")]
    embedding = object()

    client = chroma_client_module.ChromaClient(host="localhost", port=8000)
    client.from_documents(docs, embedding, "glossary_corpus")

    assert FakeChroma.last_call is not None
    assert FakeChroma.last_call["documents"] == docs
    assert FakeChroma.last_call["embedding"] == embedding
    assert FakeChroma.last_call["client"] is client.client
    assert FakeChroma.last_call["collection_name"] == "glossary_corpus"
