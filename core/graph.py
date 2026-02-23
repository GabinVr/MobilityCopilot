from langgraph.graph import StateGraph, START, END
from langgraph.prebuilt import ToolNode

from core.state import CopilotState
from core.nodes.ambiguity_detector import ambiguity_node
from core.nodes.sql_generator import sql_generator_node
from core.nodes.user_interraction import user_interaction_node
from core.nodes.synthesis import synthesis_node
from core.nodes.contradictor import contradictor_node
from core.nodes.rag import rag_node
from core.tools.tools_api_weather_now import geomet_mtl_weather_text_bundle
from core.tools.tools_api_histo import geomet_mtl_history_global_tool
from core.nodes.validator import execute_sql_node

tools = [
    geomet_mtl_weather_text_bundle,
    geomet_mtl_history_global_tool,
]

tool_node = ToolNode(tools=tools)

def route_after_ambiguity(state: CopilotState):
    if state.get("is_ambiguous"):
        return "ask_user"
    
    if state.get("need_external_data"):
        return "proceed_sql_generation" 
    
    return "bypass_to_synthesis"
    
def route_after_generator(state: CopilotState):
    messages = state.get("messages", [])
    if not messages:
        return END
    
    last_message = messages[-1]

    if hasattr(last_message, "tool_calls") and last_message.tool_calls:
        return "call_tools"

    if state.get("generated_query"):
        return "validate_query"

    return "proceed_synthesis"

def route_after_validator(state: CopilotState):
    if state.get("query_error"):
        return "sql_generator"
    
    return "synthesis"

workflow = StateGraph(CopilotState)

workflow.add_node("retriever", rag_node)
workflow.add_node("ambiguity_detector", ambiguity_node)
workflow.add_node("user_interaction", user_interaction_node)
workflow.add_node("sql_generator", sql_generator_node)
workflow.add_node("validator", execute_sql_node)
workflow.add_node("weather_tools", tool_node)
workflow.add_node("synthesis", synthesis_node)
workflow.add_node("contradictor", contradictor_node)


workflow.set_entry_point("retriever")

workflow.add_edge("retriever", "ambiguity_detector")

workflow.add_conditional_edges("ambiguity_detector", route_after_ambiguity, {
    "ask_user": "user_interaction",
    "proceed_sql_generation": "sql_generator",
    "bypass_to_synthesis": "synthesis"
})

workflow.add_conditional_edges("sql_generator", route_after_generator, {
    "call_tools": "weather_tools",
    "proceed_synthesis": "synthesis",
    "validate_query": "validator"
})


workflow.add_conditional_edges("validator", route_after_validator, {
    "sql_generator": "sql_generator",
    "synthesis": "synthesis"
})

workflow.add_edge("weather_tools", "sql_generator")
workflow.add_edge("user_interaction", END)
workflow.add_edge("synthesis", "contradictor")
workflow.add_edge("contradictor", END)

app = workflow.compile()

def get_langgraph_app():
    return app