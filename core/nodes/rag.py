from datetime import datetime

from core.state import CopilotState
from rag.repository import get_repository


def rag_node(state: CopilotState) -> CopilotState:
    """
    Node to load the whole RAG context in the state, 
    """
    repository = get_repository()

    documents = repository.get_all_documents()

    # Build a single context string while keeping metadata for traceability.
    context_chunks = []
    for doc in documents:
        meta = doc.metadata or {}
        if meta:
            context_chunks.append(f"[metadata: {meta}]\n{doc.page_content}")
        else:
            context_chunks.append(doc.page_content)

    rag_context = "\n\n---\n\n".join(context_chunks)

    today = datetime.now().strftime("%Y-%m-%d")
    rag_context = f"RAG Context as of today : {today}\n\n{rag_context}"

    return {
        "retrieved_context": rag_context,
    }





