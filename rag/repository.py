from typing import List
from rag.corpus_builder import ChromaVectorRepository, CorpusManager, VectorRepository
from utils.chroma_client import ChromaClient
from langchain_core.documents import Document
from langchain_huggingface import HuggingFaceEmbeddings
from dotenv import load_dotenv
from abc import ABC, abstractmethod
from typing import Any
import os


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

    # If embeddings are huggingface-based, we can directly instantiate them here
    # for  google-based or other types of embeddings, this part would need to be adapted
    # TODO: Add support for different types of embeddings based on environment variables
    embeddings = HuggingFaceEmbeddings(model_name=EMBEDDING_MODEL)
    return RepositoryFactory.create_chroma_repository(
        host=CHROMA_HOST,
        port=CHROMA_PORT,
        collection_name=COLLECTION_NAME,
        embeddings=embeddings
    )