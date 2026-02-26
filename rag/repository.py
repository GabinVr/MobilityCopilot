from typing import List
from utils.llm_provider import get_llm_provider_name
from rag.corpus_builder import ChromaVectorRepository, CorpusManager, VectorRepository
from utils.chroma_client import ChromaClient
from langchain_core.documents import Document
from langchain_huggingface import HuggingFaceEmbeddings
from langchain_openai import OpenAIEmbeddings
from dotenv import load_dotenv
from abc import ABC, abstractmethod
from typing import Any
import os
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class RepositoryFactory():
    @staticmethod
    def create_chroma_repository(host: str, 
                                 port: int, 
                                 collection_name: str, 
                                 embeddings: Any) -> ChromaVectorRepository:
        return ChromaVectorRepository(
            host=host,
            port=port,
            collection_name=collection_name,
            embeddings=embeddings
        )
    
def get_repository() -> VectorRepository:
    """
    Utility function to create a VectorRepository instance based on environment variables.
    """
    load_dotenv()
    CHROMA_HOST = os.getenv("CHROMA_HOST", "localhost")
    CHROMA_PORT = int(os.getenv("CHROMA_PORT", 8000))
    COLLECTION_NAME = os.getenv("COLLECTION_NAME", "glossary_corpus")
    EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "sentence-transformers/all-MiniLM-L6-v2")

    if get_llm_provider_name() == "openai":
        embeddings = OpenAIEmbeddings(
                model="text-embedding-3-large",
                # With the `text-embedding-3` class
                # of models, you can specify the size
                # of the embeddings you want returned.
                # dimensions=1024
            )
        COLLECTION_NAME = COLLECTION_NAME + "_openai"
        EMBEDDING_MODEL = "text-embedding-3-large"
    else:
        embeddings = HuggingFaceEmbeddings(model_name=EMBEDDING_MODEL)
        COLLECTION_NAME = COLLECTION_NAME + "_hf"
    logger.info(f"Chroma repository configured with host={CHROMA_HOST}, port={CHROMA_PORT}, collection_name={COLLECTION_NAME}, embedding_model={EMBEDDING_MODEL}")
    return RepositoryFactory.create_chroma_repository(
        host=CHROMA_HOST,
        port=CHROMA_PORT,
        collection_name=COLLECTION_NAME,
        embeddings=embeddings
    )