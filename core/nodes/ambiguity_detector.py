import json
import re

from core.state import CopilotState
from utils.llm_provider import get_llm
from pydantic import BaseModel, Field

class AmbiguityOutput(BaseModel):
    is_ambiguous: bool = Field(description="Indicates if the user's question is ambiguous and requires clarification.")
    clarification_options: list[str] = Field(description="If the question is ambiguous, this list contains 2-3 hypotheses or options to clarify the user's intent. If not ambiguous, this can be an empty list.")
    need_external_data: bool = Field(description="True if the question requires external data (e.g., weather with API, historical trends, statistics, 311 request) False if the question can be answered with general knowledge or reasoning without specific data.")


def ambiguity_node(state: CopilotState) -> CopilotState:
    llm = get_llm()

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
    7. If the question is clear but requires specific data to answer (e.g., "How many 311 requests were there last month in downtown?"), set need_external_data to True.
    8. If the question is clear but requires an API call to get current weather or historical trends, set need_external_data to True.
    9. If the question is clear and can be answered with general knowledge or reasoning without specific data, set need_external_data to False.
    
    """

    response = llm.with_structured_output(AmbiguityOutput).invoke(prompt)

    return {
    "is_ambiguous": response.is_ambiguous,
    "clarification_options": response.clarification_options,
    "need_external_data": response.need_external_data
    }


