import os
from typing import Optional
from langchain_community.utilities import SQLDatabase
from langchain_core.messages import HumanMessage
from core.state import CopilotState

def execute_sql_node(state: CopilotState) -> CopilotState:
    """
    Exécute la requête SQL sur la base centralisée mobility.db.
    """
    query = state.get("generated_query")
    
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
        return { 
            "query_results": str(raw_res), 
            "query_error": None
    }
    except Exception as e:
        query_error = str(e)
        feedback_ai = f"The SQL query failed with the following error: {query_error} Please correct the query accordingly."
        return {
            "query_error": query_error,
            "messages": HumanMessage(content=feedback_ai)
        }


