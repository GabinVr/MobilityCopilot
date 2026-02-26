from core.state import CopilotState
from rag.repository import get_repository
from cache import redis_cache


@redis_cache(expire=3600)  # Cache for 1 hour (documents rarely change)
def get_cached_rag_context():
    """
    Build and cache the RAG context with only JSON-serializable data.
    """
    documents = get_repository().get_all_documents()
    sources = {}

    # Build context grouped by `metadata.source` while keeping metadata for traceability.
    for doc in documents:
        meta = doc.metadata or {}
        source = str(meta.get("source") or "").strip().lower()
        if source not in sources:
            sources[source] = [(meta, doc.page_content)]
        else:
            sources[source].append((meta, doc.page_content))
        
    return sources


def rag_node(state: CopilotState) -> CopilotState:
    """
    Node to load the whole RAG context in the state.
    """
    return get_cached_rag_context()





