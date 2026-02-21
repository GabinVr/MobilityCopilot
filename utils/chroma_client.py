import chromadb
from typing import List, Dict, Optional, Any
from dotenv import load_dotenv
from langchain_core.documents import Document
from langchain_chroma import Chroma
from urllib.parse import urlparse
import logging
import os

class ChromaClient:
    """
    Simple wrapper around the ChromaDB client to manage collections and documents.
    """
    def __init__(self, chroma_url: Optional[str] = None, host: Optional[str] = None, port: Optional[int] = None):
        load_dotenv()
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

        url = chroma_url or os.getenv("CHROMA_URL")
        env_host = os.getenv("CHROMA_HOST")
        env_port = os.getenv("CHROMA_PORT")

        if url:
            parsed = urlparse(url)
            self.host = parsed.hostname or "localhost"
            self.port = parsed.port or 8000
        else:
            self.host = host or env_host or "localhost"
            self.port = port or (int(env_port) if env_port else 8000)

        self.client = chromadb.HttpClient(host=self.host, port=self.port)
        self.logger.info(f"Initialized ChromaClient with host={self.host} port={self.port}")

    def create_collection(self, name: str) -> None:
        self.client.get_or_create_collection(name)
        self.logger.info(f"Collection '{name}' is ready.")

    def get_or_create_collection(self, name: str):
        return self.client.get_or_create_collection(name)

    def delete_collection(self, name: str) -> None:
        existing = {col.name for col in self.client.list_collections()}
        if name in existing:
            self.client.delete_collection(name)
            self.logger.info(f"Collection '{name}' deleted.")
        else:
            self.logger.info(f"Collection '{name}' does not exist. No deletion performed.")

    def from_documents(self, documents: List[Document], embedding: Any, collection_name: str) -> None:
        Chroma.from_documents(
            documents=documents,
            embedding=embedding,
            client=self.client,
            collection_name=collection_name
        )
        self.logger.info(f"Added {len(documents)} documents to collection '{collection_name}'.")

    def add_documents(self, collection_name: str, documents: List[Dict[str, Any]]) -> None:
        collection = self.client.get_collection(collection_name)
        for doc in documents:
            if isinstance(doc, dict):
                collection.add(**doc)
            else:
                raise ValueError("Each document must be a dict of collection.add keyword arguments.")
        self.logger.info(f"Added {len(documents)} documents to collection '{collection_name}'.")

