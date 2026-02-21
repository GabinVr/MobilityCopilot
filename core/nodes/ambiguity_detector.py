# Node to detect ambiguities in user queries, identify potential sources of confusion.
from core.state import CopilotState
from utils.llm_provider import get_llm
from pydantic import BaseModel

class AmbiguityOutput(BaseModel):
    is_ambiguous: bool
    clarification_options: list[str]

def ambiguity_node(state: CopilotState) -> CopilotState:
    llm = get_llm()
    try:
        structured_llm = llm.with_structured_output(AmbiguityOutput)
    except Exception as e:
        prompt = f"""
        You are the Ambiguity Detector for the Montreal Mobility Copilot.
        Your task is to verify if the user's question is precise enough to be translated into a SQL query.
        OFFICIAL GLOSSARY & SCHEMAS (RAG):
        {state.get("retrieved_context")}
        USER QUESTION:
        {state.get("messages")[-1].content if state.get("messages") else ""}
        HISTORY OF CONVERSATION:
        {state.get("messages")}
        INSTRUCTIONS:
        1. Analyze the user's question in the context of the provided glossary and conversation history.
        2. Check if the question lacks a specific location, time period, incident type or any other detail that would make it hard to generate a precise query.
        3. IMPORTANT: If the user is responding to a previous clarification or providing a missing detail, use the history to resolve the ambiguity instead of marking it as ambiguous again.
        4. If the question is "fuzzy" (e.g., "where are the problems?"), set is_ambiguous to True.
        5. If ambiguous, use the GLOSSARY to propose 2-3 hypotheses (e.g., "Do you mean 311 pothole requests or severe collisions?").
        6. If the question is clear and matches categories in the glossary, set is_ambiguous to False.

        OUTPUT FORMAT:
        {{
    "is_ambiguous": boolean,
    "clarification_options": [list of 2-3 strings with clarification options if ambiguous, empty list if not ambiguous]
}}
    """
        response = llm.invoke(prompt)
        return {
            "is_ambiguous": response.is_ambiguous,
            "clarification_options": response.clarification_options
        }
    rag_context = state.get("retrieved_context")
    messages = state.get("messages")

    if messages:
        question = messages[-1].content
    else:        question = ""

    prompt = f"""
    You are the Ambiguity Detector for the Montreal Mobility Copilot.
    Your task is to verify if the user's question is precise enough to be translated into a SQL query.

    OFFICIAL GLOSSARY & SCHEMAS (RAG):
    {rag_context}

    USER QUESTION:
    {question}

    HISTORY OF CONVERSATION:
    {messages}

    INSTRUCTIONS:
    1. Analyze the user's question in the context of the provided glossary and conversation history.
    2. Check if the question lacks a specific location, time period, incident type or any other detail that would make it hard to generate a precise query.
    3. IMPORTANT: If the user is responding to a previous clarification or providing a missing detail, use the history to resolve the ambiguity instead of marking it as ambiguous again.
    4. If the question is "fuzzy" (e.g., "where are the problems?"), set is_ambiguous to True.
    5. If ambiguous, use the GLOSSARY to propose 2-3 hypotheses (e.g., "Do you mean 311 pothole requests or severe collisions?").
    6. If the question is clear and matches categories in the glossary, set is_ambiguous to False.
    """

    response = structured_llm.invoke(prompt)

    return {
        "is_ambiguous": response.is_ambiguous,
        "clarification_options": response.clarification_options
    }


