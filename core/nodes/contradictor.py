from core.state import CopilotState
from utils.llm_provider import get_llm
from langchain_core.messages import SystemMessage, HumanMessage

def contradictor_node(state: CopilotState) -> CopilotState:
    llm = get_llm()
    if llm is None:
        return {
            "contradictor_notes": None,
            "error": "LLM is not configured.",
        }
    
    final_answer = state.get("analytical_response")
    query_results = state.get("query_results", "No SQL results available.")
    rag_context = state.get("retrieved_context", "No context available.")

    system_prompt = """
    You are the MobilityCopilot security auditor.
    Your role is to provide a critical perspective and nuance to the final response

    YOUR TASKS:
    1. Identify limitations : if the SQL data is empty or incomplete, highlight this and explain how it affects the final answer.
    2. Cite the sources : is informations come from 311, remind that 311 data is based on user reports and may not be comprehensive or fully accurate.
    3. Security alert : always emphasize that real-world condition take precedence over the application.
    4. Detect uncertainty : if the copilot seems overconfident when data is uotdated or incomplete, moderate their statements and highlight the need for human judgement.

    INSTRUCTIONS:
    1. Produce a short report (2-3 sentences)
    2. Be constructive but firm on the need for caution.
    3. Speak in a clear and concise manner, suitable for a general audience, use simple language and avoid technical jargon like 'SQL' or 'database'.
    4. Speak in the user's language (French or English) based on the conversation history.
    """

    prompt_context = f"""
    Final answer: {final_answer}
    SQL results: {query_results}
    RAG context: {rag_context}
    Generate the contradictor report based on the above information and the system prompt.
    """

    response = llm.invoke([SystemMessage(content=system_prompt), HumanMessage(content=prompt_context)])

    return {
        "contradictor_notes": response.content,
        }