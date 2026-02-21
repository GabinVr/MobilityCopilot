import importlib
import json
import sys
import types
from types import SimpleNamespace

import pytest


class FakeDocument:
    def __init__(self, page_content: str, metadata=None) -> None:
        self.page_content = page_content
        self.metadata = metadata or {}


def _import_corpus_builder_module():
    lc_docs_mod = sys.modules.setdefault(
        "langchain_core.documents", types.ModuleType("langchain_core.documents")
    )
    setattr(lc_docs_mod, "Document", FakeDocument)

    lc_embeddings_mod = sys.modules.setdefault(
        "langchain_core.embeddings", types.ModuleType("langchain_core.embeddings")
    )
    setattr(lc_embeddings_mod, "Embeddings", object)

    hf_mod = sys.modules.setdefault(
        "langchain_huggingface", types.ModuleType("langchain_huggingface")
    )
    setattr(hf_mod, "HuggingFaceEmbeddings", object)

    chromadb_mod = sys.modules.setdefault("chromadb", types.ModuleType("chromadb"))
    setattr(chromadb_mod, "HttpClient", object)

    langchain_chroma_mod = sys.modules.setdefault(
        "langchain_chroma", types.ModuleType("langchain_chroma")
    )
    setattr(langchain_chroma_mod, "Chroma", object)

    module = importlib.import_module("rag.corpus_builder")
    return importlib.reload(module)


@pytest.fixture
def corpus_builder_module():
    return _import_corpus_builder_module()


def test_json_directory_document_provider_reads_files(
    tmp_path, corpus_builder_module
) -> None:
    payload = [
        {
            "content": "Dataset 311 description",
            "metadata": {"source": "dataset_description", "dataset": "311"},
        }
    ]
    json_file = tmp_path / "sample.json"
    json_file.write_text(json.dumps(payload), encoding="utf-8")

    provider = corpus_builder_module.JsonDirectoryDocumentProvider(
        directory_path=str(tmp_path)
    )
    docs = provider.get_documents()

    assert len(docs) == 1
    assert docs[0].page_content == "Dataset 311 description"
    assert docs[0].metadata["dataset"] == "311"


def test_chroma_vector_repository_get_all_documents(monkeypatch, corpus_builder_module) -> None:
    class FakeCollection:
        def get(self, include=None):
            return {
                "documents": ["Doc A", "Doc B"],
                "metadatas": [{"source": "a"}, {"source": "b"}],
            }

    class FakeChromaClient:
        def __init__(self, host=None, port=None):
            self.host = host
            self.port = port

        def get_or_create_collection(self, _name):
            return FakeCollection()

    monkeypatch.setattr(corpus_builder_module, "ChromaClient", FakeChromaClient)

    repo = corpus_builder_module.ChromaVectorRepository(
        host="localhost",
        port=8000,
        collection_name="glossary_corpus",
        embeddings=SimpleNamespace(),
    )

    docs = repo.get_all_documents()

    assert [doc.page_content for doc in docs] == ["Doc A", "Doc B"]
    assert docs[0].metadata["source"] == "a"
