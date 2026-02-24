import os
from typing import Optional
from langchain_community.utilities import SQLDatabase
from langchain_core.messages import HumanMessage
from core.state import CopilotState

def execute_sql_node(state: CopilotState) -> CopilotState:
    """
    Exécute la requête SQL sur la base centralisée mobility.db.
    """
    messages = state.get("messages", [])
    last_message = messages[-1] if messages else None
    sql_query = state.get("generated_query") or ""

    if (
        last_message is not None
        and getattr(last_message, "type", None) == "tool"
        and getattr(last_message, "name", None) == "generate_and_validate_sql"
    ):

        raw_content = last_message.content
        sql_query = ""

        if "VALID_SQL_READY_TO_EXECUTE:" in raw_content:
            sql_query = raw_content.replace("VALID_SQL_READY_TO_EXECUTE:", "").strip()
        else:
            sql_query = raw_content.strip()

    query = sql_query
    
    # --- Gestion du chemin Robuste ---
    # On remonte de core/nodes/ vers la racine pour atteindre data/db/
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(os.path.dirname(current_dir))
    db_path = os.path.join(project_root, "data", "db", "mobility.db")

    if not query:
        # On évite d'écraser tout le state, on ne renvoie que les changements
        return {"query_error": "Requête vide ou non générée."}

    try:
        # 1. Connexion (Utilise l'URI absolue avec 4 slashs pour SQLite sur certains OS)
        db = SQLDatabase.from_uri(f"sqlite:///{db_path}")
        
        # 2. Exécution
        # On force en string pour éviter les erreurs de type Pylance
        raw_res = db.run(query)

        sucess_message = f"SQL execution successful. Here are the results : \n{raw_res}\n\nIf you have all the information you need, formulate your final answer in plain text. If you need weather data, call the weather tool now. And if you need other SQL data, use the SQL tool again."
        return { 
            "query_results": str(raw_res), 
            "query_error": None,
            "generated_query": None,
            "messages": HumanMessage(content=sucess_message)
    }
    except Exception as e:
        query_error = str(e)
        feedback_ai = f"The SQL query failed with the following error: {query_error} Please correct the SQL syntax and try again."
        return {
            "query_error": query_error,
            "generated_query": None,
            "messages": HumanMessage(content=feedback_ai)
        }


