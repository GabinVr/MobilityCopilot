import os
import logging
import threading
from langchain_core.messages import HumanMessage
from core.graph import get_langgraph_app

logger = logging.getLogger("uvicorn.error")


class LastHotspotReport:
    """
    Utility class to store and manage the latest generated mobility hotspot report.
    Thread-safe and persists to a local file.
    """
    def __init__(self, path: str = "last_hotspot_report.txt"):
        self.path = path
        self.lock = threading.RLock()
        if os.path.exists(self.path):
            with open(self.path, "r", encoding="utf-8") as file:
                self.report = file.read()
        else:
            self.report = "Aucun rapport généré pour le moment."

    def update_report(self, new_report: str):
        with self.lock:
            self.report = new_report
            with open(self.path, "w", encoding="utf-8") as file:
                file.write(new_report)

    def get_report(self) -> str:
        with self.lock:
            return self.report


# Module-level singleton instance and accessor helpers to avoid tight coupling.
_last_hotspot_instance = LastHotspotReport()


def get_last_hotspot_report() -> str:
    """Return the last generated hotspot report as a string."""
    return _last_hotspot_instance.get_report()


def update_last_hotspot_report(new_report: str) -> None:
    """Update and persist the last hotspot report."""
    _last_hotspot_instance.update_report(new_report)


def hebdo_hotspots_briefing_generator():
    """Générer un briefing hebdomadaire des hotspots de mobilité à Montréal et le stocker via l'accessor."""
    prompt = """
        Generate the weekly mobility hotspots briefing for Montreal.
    
    TASK:
    1. Analyze the mobility data (collisions, 311 requests, etc.) and weather conditions provided in the context for the last 7 days.
    2. Identify the TOP 5 absolute worst hotspots based on the highest concentration of problems.
    
    CRITICAL ANTI-HALLUCINATION RULES:
    - You MUST rely EXCLUSIVELY on the data retrieved by the SQL queries and API tools.
    - IF THE DATA IS EMPTY or insufficient to find 5 hotspots, DO NOT INVENT or hallucinate locations. 
    - If there is absolutely no data, your response must be exactly: "Aucun incident ou requête majeure n'a été enregistré dans nos bases de données pour cette semaine."
    - If you only find 2 hotspots in the real data, only output 2. Do not invent the remaining 3.
    
    STRICT FORMATTING RULES (Only for REAL data found):
    - Hotspot #[Rank] : [Location/Zone] - [Total Number] [Type of Issue] ([Specific details]), [Time/Duration/Context], [Weather condition if relevant].
    
    EXAMPLES OF EXPECTED OUTPUT (Do NOT copy these, use them only as a formatting guide):
    - Hotspot #1 : Intersection Peel/Ste-Catherine - 32 collisions (dont 6 graves), surtout entre 16h-19h, sous la pluie.
    - Hotspot #2 : Secteur Plateau-Mont-Royal - 120 requêtes 311 (Nids-de-poule) en 7 jours.
    
    IMPORTANT: 
    - Output ONLY the formatted list based on REAL data.
    - Respond strictly in French.
    """
    
    initial_state = {
        "messages": [HumanMessage(content=prompt)],
        "audience": "grand_public",
        "is_ambiguous": False
    }

    no_data_message = "Aucun incident ou requête majeure n'a été enregistré dans nos bases de données pour cette semaine."

    try:
        langgraph_app = get_langgraph_app()
        resultats = langgraph_app.invoke(initial_state)

        rapport = resultats.get("analytical_response", "Erreur de génération.")
        notes = resultats.get("contradictor_notes", "")

        if rapport == no_data_message:
            update_last_hotspot_report(no_data_message)
        else:
            update_last_hotspot_report(
                f"Voici le rapport hebdomadaire des hotspots de mobilité à Montréal :\n\n{rapport}\n\nNotes de sécurité : {notes}"
            )

    except Exception as e:
        logger.exception("Erreur lors de la génération du rapport")
        update_last_hotspot_report(f"Erreur lors de la génération du rapport : {e}")
