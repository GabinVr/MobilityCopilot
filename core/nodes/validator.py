import os
from typing import Optional
from langchain_community.utilities import SQLDatabase
from core.state import CopilotState

def execute_sql_node(state: CopilotState) -> CopilotState:
    """
    Exécute la requête SQL sur la base centralisée mobility.db.
    """
    query = "SELECT name FROM sqlite_master WHERE type='table';"
    
    # --- Gestion du chemin Robuste ---
    # On remonte de core/nodes/ vers la racine pour atteindre data/db/
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(os.path.dirname(current_dir))
    db_path = os.path.join(project_root, "data", "db", "mobility.db")
    
    query_results: Optional[str] = None
    query_error: Optional[str] = None

    if not query:
        # On évite d'écraser tout le state, on ne renvoie que les changements
        return {**state, "query_error": "Requête vide ou non générée."}

    try:
        # 1. Connexion (Utilise l'URI absolue avec 4 slashs pour SQLite sur certains OS)
        db = SQLDatabase.from_uri(f"sqlite:///{db_path}")
        
        # 2. Exécution
        # On force en string pour éviter les erreurs de type Pylance
        raw_res = db.run(query)
        query_results = str(raw_res)
        
    except Exception as e:
        query_error = str(e)

    return {
        **state, 
        "query_results": query_results, 
        "query_error": query_error
    }


