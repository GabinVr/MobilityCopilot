from typing import TypedDict, Annotated, List, Optional
from langgraph.graph.message import add_messages
from langchain_core.messages import AnyMessage

class CopilotState(TypedDict):
    # --- 1. Core Conversation ---
    # Annotated with add_messages so LangGraph automatically appends new messages 
    # instead of overwriting the history. This is crucial for the chat interface.
    messages: Annotated[List[AnyMessage], add_messages]
    
    # Track who we are talking to: "grand_public" vs "municipalite"
    audience: str 
    
    # --- 2. Ambiguity Detection ---
    # To satisfy the requirement to detect fuzzy queries and act on them.
    is_ambiguous: bool
    clarification_options: Optional[str] # The hypotheses proposed to the user
    
    # --- 3. RAG Grounding ---
    # Holds the glossary/dataset definitions retrieved to avoid hallucinations.
    retrieved_context: str
    
    # --- 4. Query Generation & Execution Loop ---
    # The LLM must produce a query (SQL/pandas) and a validator must run it.
    generated_query: Optional[str]
    query_results: Optional[str] # The raw output from the database/dataframe
    query_error: Optional[str]   # If the code fails, we feed this back to the LLM to fix it
    
    # --- 5. Final Synthesis & Contradictor ---
    # The analytical summary of the data.
    analytical_response: Optional[str]
    # The required "Contradictor mode" notes (limits, risks, next steps to verify).
    contradictor_notes: Optional[str]