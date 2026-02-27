from typing import List, Any
from utils.llm_provider import get_llm_provider_name, get_embedding_model
from rag.corpus_builder import ChromaVectorRepository, CorpusManager, VectorRepository
from utils.chroma_client import ChromaClient
from langchain_core.documents import Document
from dotenv import load_dotenv
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

    embeddings = get_embedding_model()
    if get_llm_provider_name() == "openai":
        COLLECTION_NAME = COLLECTION_NAME + "_openai"
    else:
        COLLECTION_NAME = COLLECTION_NAME + "_hf"
    logger.info(f"Chroma repository configured with host={CHROMA_HOST}, port={CHROMA_PORT}, collection_name={COLLECTION_NAME}, embedding_model={EMBEDDING_MODEL}")
    return RepositoryFactory.create_chroma_repository(
        host=CHROMA_HOST,
        port=CHROMA_PORT,
        collection_name=COLLECTION_NAME,
        embeddings=embeddings
    )