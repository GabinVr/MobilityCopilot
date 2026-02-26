from typing import List
from abc import ABC, abstractmethod
from dotenv import load_dotenv
import os
import glob
import json
import logging
from langchain_core.documents import Document
from langchain_huggingface import HuggingFaceEmbeddings
from langchain_openai import OpenAIEmbeddings
from langchain_core.embeddings import Embeddings
from langchain_chroma import Chroma
from utils.chroma_client import ChromaClient

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

## ABSTRACT CLASSES ##

class DocumentProvider(ABC):
    """Interface for providing documents to be ingested into the vector database."""
    @abstractmethod
    def get_documents(self) -> List[Document]:
        pass

class VectorRepository(ABC):
    """Interface for a vector repository to store and retrieve document embeddings."""
    @abstractmethod
    def clear(self) -> None:
        pass

    @abstractmethod
    def save(self, documents: List[Document]) -> None:
        pass

    @abstractmethod
    def query(self, query_texts: List[str], n_results: int) -> List[Document]:
        pass

    @abstractmethod
    def get_all_documents(self) -> List[Document]:
        pass



## IMPLEMENTATIONS ##

class JsonDirectoryDocumentProvider(DocumentProvider):
    """
    Reads JSON files from a specified directory and converts them into Document objects.
    Each JSON file is expected to contain a list of entries, where each entry has a 'content' field and a 'metadata' field.
    Example JSON structure:
    [
    {
        "content": "Le jeu de données des requêtes 311 contient l'historique des demandes de services des citoyens à la Ville de Montréal. Il inclut le type de demande, la date d'ouverture, et le secteur.",
        "metadata": {"source": "dataset_description", "dataset": "311"}
    }
    ]
    """
    def __init__(self, directory_path: str = "data/glossaries/"):
        self.directory_path = directory_path

    def get_documents(self) -> List[Document]:
        documents = []
        
        json_files = glob.glob(os.path.join(self.directory_path, "*.json"))
        
        for file_path in json_files:
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    
                    for item in data:
                        doc = Document(
                            page_content=item.get("content", ""),
                            metadata=item.get("metadata", {})
                        )
                        documents.append(doc)
                        
            except Exception as e:
                logger.error(f"Error while reading {file_path}: {e}")
                
        return documents
    
class ChromaVectorRepository(VectorRepository):
    """
    Implementation of VectorRepository using ChromaDB.
    """
    def __init__(self, host: str, port: int, collection_name: str, embeddings: Embeddings):
        self.collection_name = collection_name
        self.embeddings = embeddings
        self.client = ChromaClient(host=host, port=port)
        self.client.get_or_create_collection(collection_name)
        self._vectorstore = None

    def clear(self) -> None:
        try:
            self.client.delete_collection(self.collection_name)
            self._vectorstore = None
            logger.info(f"Collection '{self.collection_name}' cleared successfully.")
        except Exception:
            logger.info(f"Collection '{self.collection_name}' does not exist or could not be deleted. Proceeding with creation.")

    def save(self, documents: List[Document]) -> None:
        logger.info(f"Insertion de {len(documents)} documents dans '{self.collection_name}'...")
        self.client.from_documents(
            documents=documents,
            embedding=self.embeddings,
            collection_name=self.collection_name
        )
        # Reset vectorstore so it's recreated on next query
        self._vectorstore = None
        logger.info("Documents sauvegardés avec succès.")

    def _get_vectorstore(self) -> Chroma:
        """Get or create the Langchain Chroma vectorstore with the correct embeddings."""
        if self._vectorstore is None:
            self._vectorstore = Chroma(
                client=self.client.client,
                collection_name=self.collection_name,
                embedding_function=self.embeddings
            )
        return self._vectorstore

    def query(self, query_texts: List[str], n_results: int) -> List[Document]:
        vectorstore = self._get_vectorstore()
        # Use similarity_search which handles embeddings correctly
        docs = []
        for query_text in query_texts:
            results = vectorstore.similarity_search(query_text, k=n_results)
            docs.extend(results)
        return docs

    def get_all_documents(self) -> List[Document]:
        vectorstore = self._get_vectorstore()
        # Get all documents from the collection
        collection = self.client.get_or_create_collection(self.collection_name)
        results = collection.get(include=["documents", "metadatas"])
        documents = results.get("documents", []) or []
        metadatas = results.get("metadatas", []) or []

        docs: List[Document] = []
        for idx, content in enumerate(documents):
            metadata = metadatas[idx] if idx < len(metadatas) else {}
            docs.append(Document(page_content=content, metadata=metadata))
        return docs
    
## Orchestrator

class CorpusManager:
    """
    Orchestrator class that builds the RAG corpus by fetching documents from a DocumentProvider and saving them into a VectorRepository.
    """
    def __init__(self, doc_provider: DocumentProvider, repository: VectorRepository):
        self.doc_provider = doc_provider
        self.repository = repository

    def build_corpus(self) -> None:
        logger.info("Starting corpus building process...")
        documents = self.doc_provider.get_documents()
        self.repository.clear()
        self.repository.save(documents)
        logger.info("Corpus building process completed.")

if __name__ == "__main__":
    load_dotenv()

    CHROMA_HOST = os.getenv("CHROMA_HOST", "localhost")
    CHROMA_PORT = int(os.getenv("CHROMA_PORT", 8000))
    COLLECTION_NAME = "glossary_corpus"

    from utils.llm_provider import get_embedding_model
    doc_provider = JsonDirectoryDocumentProvider(directory_path="data/glossaries/")
    embeddings = get_embedding_model()
    repository = ChromaVectorRepository(
        host=CHROMA_HOST,
        port=CHROMA_PORT,
        collection_name=COLLECTION_NAME,
        embeddings=embeddings
    )
    
    corpus_manager = CorpusManager(doc_provider, repository)
    corpus_manager.build_corpus()

    # Test retrieval (optional)
    results = repository.client.get_or_create_collection(COLLECTION_NAME).query(
        query_texts=["Qu'est-ce que le dataset 311?"],
        n_results=3
    )
    logger.info(f"Test retrieval results: {results}")

