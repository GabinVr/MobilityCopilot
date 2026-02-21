from langgraph.graph import StateGraph, START, END
from core.state import CopilotState
from core.nodes.ambiguity_detector import ambiguity_node
from core.nodes.sql_generator import sql_generator_node
from core.nodes.user_interraction import user_interaction_node
from core.nodes.synthesis import synthesis_node
from core.nodes.contradictor import contradictor_node
from core.nodes.retriever import retriever_node


def route_after_ambiguity(state: CopilotState):
    if state.get("is_ambiguous"):
         # If fuzzy, we end early to ask the user to clarify/choose an hypothesis
        return "ask_user"
    # Otherwise, proceed to generate query
    return "proceed_sql_generation" 

workflow = StateGraph(CopilotState)

workflow.add_node("retriever", retriever_node)
workflow.add_node("ambiguity_detector", ambiguity_node)

workflow.add_node("sql_generator", sql_generator_node)
workflow.add_node("user_interaction", user_interaction_node)
workflow.add_node("synthesis", synthesis_node)
workflow.add_node("contradictor", contradictor_node)

workflow.set_entry_point("retriever")

workflow.add_edge("retriever", "ambiguity_detector")

workflow.add_conditional_edges("ambiguity_detector", route_after_ambiguity, {
    "ask_user": END,   # Ou breakpoint si on veut faire du live avec l'utilisateur
    "proceed_sql_generation": "sql_generator"
})


workflow.add_edge("sql_generator", "synthesis")
workflow.add_edge("synthesis", "contradictor")
workflow.add_edge("contradictor", END)

app = workflow.compile()

