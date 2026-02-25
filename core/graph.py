from langgraph.graph import StateGraph, START, END
from langgraph.prebuilt import ToolNode
from core.state import CopilotState
from core.nodes.ambiguity_detector import ambiguity_node
from core.nodes.data_agent import data_agent_node
from core.nodes.user_interraction import user_interaction_node
from core.nodes.synthesis import synthesis_node
from core.nodes.contradictor import contradictor_node
from core.nodes.rag import rag_node
from core.tools.tools_api_weather_now import geomet_mtl_weather_text_bundle
from core.tools.tools_api_histo import geomet_mtl_history_global_tool
from core.tools.sql_generator import sql_generator_tool
from core.nodes.validator import execute_sql_node

tools = [
    geomet_mtl_weather_text_bundle,
    geomet_mtl_history_global_tool,
    sql_generator_tool,
]

tool_node = ToolNode(tools=tools)

def route_after_ambiguity(state: CopilotState):
    if state.get("is_ambiguous"):
        return "ask_user"
    
    if state.get("need_external_data"):
        return "proceed_data_agent"
    
    return "bypass_to_synthesis"
    
def route_after_data_agent(state: CopilotState):
    messages = state.get("messages", [])
    if not messages:
        return END
    
    last_message = messages[-1]

    if hasattr(last_message, "tool_calls") and last_message.tool_calls:
        return "call_tools"

    if last_message.content and "DATA GATHERING COMPLETE:" in last_message.content:
        return "proceed_synthesis"
    
    if state.get("query_error"):
        return "loop_back_data_agent"

    return "proceed_synthesis"

def route_after_tools(state: CopilotState):
    
    messages = state.get("messages", [])

    for msg in reversed(messages):
        if msg.type == "ai":
            break
            
        if msg.type == "tool":
            if msg.name == "generate_and_validate_sql":
                return "validator_node"
    return "data_agent"

# This function is kept for backwards compatibility.
def get_langgraph_app():
    workflow = build_workflow()
    app = workflow.compile()
    return app

def build_workflow():
    workflow = StateGraph(CopilotState)

    workflow.add_node("retriever", rag_node)
    workflow.add_node("ambiguity_detector", ambiguity_node)
    workflow.add_node("user_interaction", user_interaction_node)
    workflow.add_node("data_agent", data_agent_node)
    workflow.add_node("validator", execute_sql_node)
    workflow.add_node("tools_node", tool_node)
    workflow.add_node("synthesis", synthesis_node)
    workflow.add_node("contradictor", contradictor_node)

    workflow.add_edge(START, "retriever")
    workflow.add_edge("retriever", "ambiguity_detector")

    workflow.add_conditional_edges("ambiguity_detector", route_after_ambiguity, {
        "ask_user": "user_interaction",
        "proceed_data_agent": "data_agent",
        "bypass_to_synthesis": "synthesis"
    })

    workflow.add_conditional_edges("data_agent", route_after_data_agent, {
        "call_tools": "tools_node",
        "proceed_synthesis": "synthesis",
        "loop_back_data_agent": "data_agent"
    })

    workflow.add_conditional_edges("tools_node", route_after_tools, {
        "validator_node": "validator",
        "data_agent": "data_agent"
    })
    workflow.add_edge("validator", "data_agent")
    workflow.add_edge("synthesis", "contradictor")
    workflow.add_edge("contradictor", END)
    workflow.add_edge("user_interaction", END)

    return workflow