from core.state import CopilotState
from utils.llm_provider import get_llm
from langchain_core.messages import SystemMessage, HumanMessage
from langchain_core.runnables import RunnableConfig

# def synthesis_node(state: CopilotState, config: RunnableConfig) -> CopilotState:
def synthesis_node(state: CopilotState, config: RunnableConfig) -> CopilotState:
    llm = get_llm()

    # audience = state.get("audience", "grand public") #  --> moved to RunnableConfig
    audience = config["configurable"]["audience"] 
    if audience not in ["grand_public", "municipalite"]:
        audience = "grand_public" # default fallback

    messages = state.get("messages", [])
    
    business_rules = state.get("business_rules", "No business rules found.")
    question = state.get("question", "No question found.")

    chat_history_text = ""
    for m in messages:
        role = "Utilisateur" if m.type == "human" else "Assistant"
        chat_history_text += f"{role}: {m.content}\n"

    style_guide = ("Answer in a clear and concise manner, suitable for a general audience, use simple language and avoid technical jargon. "
                   if audience == "grand public" else
                   "Answer with precision and technical depth, suitable for a specialized audience, using appropriate terminology and detailed explanations.")
    
    system_prompt = f"""
    You are the Synthesis Agent for the Montreal Mobility Copilot.
    Your task is to write the final analytical report based strictly on the raw data gathered by the Data Agent in the conversation history.
    If the question has no relevance to mobility, requests 311 or weather in Montreal, say that you cannot answer and stop.

    RAW DATA & RULES:
    1. BUSINESS RULES & DEFINITIONS: {business_rules}
    2. GATHERED DATA (History): {chat_history_text}

    The question to answer is: "{question}"

    STYLE GUIDE:
    {style_guide}

    INSTRUCTIONS:
    1. Base your answer ONLY on the data provided in the GATHERED DATA section. 
    2. Pay special attention to the message starting with "DATA GATHERING COMPLETE".
    3. Follow the STYLE GUIDE above for audience adaptation.
    4. Never hallucinate information that is not explicitly stated in the gathered data. If you don't have enough information to answer, say "Je n'ai pas assez d'informations pour répondre à cette question." and stop.
    5. NEVER say 'SQL', 'database', 'dataframe', 'API', or 'Data Agent'. Speak as the primary mobility expert.
    6. Always reply in French, as this is for the City of Montreal.
    """

    final_message = [
        SystemMessage(content=system_prompt),
        HumanMessage(content="Based on the above data, generate the final analytical report for the user following the instructions and structure guidelines.")
    ]

    response = llm.invoke(final_message)

    return {
        "messages": [response],
        "analytical_response": response.content
        }