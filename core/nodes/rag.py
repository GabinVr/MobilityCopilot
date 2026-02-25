from core.state import CopilotState
from rag.repository import get_repository
from cache import redis_cache


@redis_cache(expire=3600)  # Cache for 1 hour (documents rarely change)
def get_cached_rag_context():
    """
    Build and cache the RAG context with only JSON-serializable data.
    """
    documents = get_repository().get_all_documents()
    db_schema_docs = []
    sql_help_docs = []
    business_docs = []
    table_desc_docs = []

    # Build context grouped by `metadata.source` while keeping metadata for traceability.
    for doc in documents:
        meta = doc.metadata or {}
        source = str(meta.get("source") or "").strip().lower()

        if source == "database_schema":
            db_schema_docs.append((meta, doc.page_content))
        elif source == "querying_tips":
            sql_help_docs.append((meta, doc.page_content))
        elif source == "business_rules":
            business_docs.append((meta, doc.page_content))
        elif source == "dataset_description":
            table_desc_docs.append((meta, doc.page_content))

    return {
        "database_schema": db_schema_docs,
        "querying_tips": sql_help_docs,
        "business_rules": business_docs,
        "table_descriptions": table_desc_docs,
    }


def rag_node(state: CopilotState) -> CopilotState:
    """
    Node to load the whole RAG context in the state.
    """
    return get_cached_rag_context()





