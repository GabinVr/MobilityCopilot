from langgraph.graph import StateGraph, START, END
from core.state import CopilotState
from core.nodes.ambiguity_detector import ambiguity_node
from core.nodes.sql_generator import sql_generation_node
from core.nodes.user_interraction import user_interaction_node
from core.nodes.synthesis import synthesis_node
from core.nodes.contradictor import contradictor_node

def route_after_ambiguity(state: CopilotState):
    if state.get("is_ambiguous"):
         # If fuzzy, we end early to ask the user to clarify/choose an hypothesis
        return END
    # Otherwise, proceed to generate query
    return "sql_generator" 

workflow = StateGraph(CopilotState)

workflow.add_node("detect_ambiguity", ambiguity_node)
workflow.add_node("sql_generator", sql_generation_node)
workflow.add_node("user_interaction", user_interaction_node)
workflow.add_node("synthesis", synthesis_node)
workflow.add_node("contradictor", contradictor_node)

workflow.add_edge(START, "detect_ambiguity")

workflow.add_conditional_edges("detect_ambiguity", route_after_ambiguity)

workflow.add_edge("sql_generator", "synthesis")
workflow.add_edge("synthesis", "contradictor")
workflow.add_edge("contradictor", END)

app = workflow.compile()

