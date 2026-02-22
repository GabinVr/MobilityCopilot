from core.state import CopilotState
from utils.llm_provider import get_llm
from langchain_core.messages import SystemMessage, HumanMessage

def synthesis_node(state: CopilotState) -> CopilotState:
    llm = get_llm()

    audience = state.get("audience", "grand public")
    rag_context = state.get("retrieved_context", "No context available")
    sql_results = state.get("sql_results", "No SQL results available")
    messages = state.get("messages", [])

    chat_history_text = ""
    for m in messages:
        role = "Utilisateur" if m.type == "human" else "Assistant"
        chat_history_text += f"{role}: {m.content}\n"

    style_guide = ("Answer in a clear and concise manner, suitable for a general audience, use simple language and avoid technical jargon. "
                   if audience == "grand public" else
                   "Answer with precision and technical depth, suitable for a specialized audience, using appropriate terminology and detailed explanations.")
    
    system_prompt = f"""
    You are the Synthesis Agent for the Montreal Mobility Copilot.
    Your task is to synthesize a final answer to the user's question based on the following information:
    1. RAG CONTEXT : {rag_context}
    2. SQL RESULTS : {sql_results}
    3. CONVERSATION HISTORY : {chat_history_text}

    INSTRUCTIONS:
    1. Use the SQL results as the primary source of factual information to answer the user's question.
    2. Use the RAG context to provide background information or definitions.SystemError
    3. Use the conversation history to understand the user's intent and any clarifications that were made.
    4. STYLE GUIDE: {style_guide}
    5. Never hallucinate information. If you don't know, say you don't know.
    6. If weather API results are present, integrate them into the answer in a natural way.
    7. Don't say 'SQL', 'database', 'dataframe' or any technical term in the final answer. Just use the information to answer the question.
    8. Speak in the user's language (French or English) based on the conversation history.
    """

    response = llm.invoke([HumanMessage(content=system_prompt)])

    return {
        "messages": [response],
        "analytical_response": response.content
        }