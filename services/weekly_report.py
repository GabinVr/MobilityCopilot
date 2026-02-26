import os
import json
import logging
import threading
import time
from langchain_core.messages import HumanMessage
from core.graph import get_langgraph_app

logger = logging.getLogger("uvicorn.error")


def _split_report_and_recommendations_text(content: str, language: str) -> tuple[str, str]:
    """Split report and recommendation sections by language marker."""
    marker = "Recommandations :" if language == "fr" else "Recommendations:"
    if not isinstance(content, str):
        return "", ""
    idx = content.find(marker)
    if idx < 0:
        return content.strip(), ""
    return content[:idx].strip(), content[idx + len(marker) :].strip()


class LastWeeklyReportStore:
    """
    Thread-safe store for weekly mobility reports in one JSON file.

    Structure:
    {
      "updated_at": "...",
            "reports": {
                "hotspot": {"fr": "...", "en": "...", "reco_fr": "...", "reco_en": "..."},
                "weak_signal": {"fr": "...", "en": "...", "reco_fr": "...", "reco_en": "..."}
            }
    }
    """
    def __init__(self, path: str = "data/report/last_weekly_reports.json"):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        self.path = path
        self.lock = threading.RLock()

        self.reports = {
            "hotspot": {
                "fr": "Aucun rapport généré pour le moment.",
                "en": "No report has been generated yet.",
                "reco_fr": "",
                "reco_en": "",
            },
            "weak_signal": {
                "fr": "Aucun signal faible généré pour le moment.",
                "en": "No weak signal report has been generated yet.",
                "reco_fr": "",
                "reco_en": "",
            },
        }

        self._load_from_disk()

    def _normalize_language(self, language: str) -> str:
        if not language:
            return "fr"
        language = language.strip().lower()
        if language.startswith("en"):
            return "en"
        return "fr"

    def _normalize_report_type(self, report_type: str) -> str:
        if not report_type:
            return "hotspot"
        value = report_type.strip().lower().replace("-", "_")
        if value in {"weak_signal", "weak_signals", "weaksignal"}:
            return "weak_signal"
        return "hotspot"

    def _load_from_disk(self) -> None:
        if not os.path.exists(self.path):
            return

        try:
            with open(self.path, "r", encoding="utf-8") as file:
                payload = json.load(file)

            if not isinstance(payload, dict):
                return

            stored_reports = payload.get("reports", {})
            if not isinstance(stored_reports, dict):
                return

            for report_type in ("hotspot", "weak_signal"):
                by_language = stored_reports.get(report_type, {})
                if not isinstance(by_language, dict):
                    continue
                for language in ("fr", "en"):
                    report = by_language.get(language)
                    if isinstance(report, str):
                        main_report, extracted_reco = _split_report_and_recommendations_text(report, language)
                        self.reports[report_type][language] = main_report
                        reco_key = "reco_fr" if language == "fr" else "reco_en"
                        if extracted_reco:
                            self.reports[report_type][reco_key] = extracted_reco

                reco_fr = by_language.get("reco_fr")
                reco_en = by_language.get("reco_en")
                if isinstance(reco_fr, str):
                    self.reports[report_type]["reco_fr"] = reco_fr
                if isinstance(reco_en, str):
                    self.reports[report_type]["reco_en"] = reco_en
        except Exception:
            logger.exception("Impossible de charger les rapports hebdomadaires")

    def _write_to_disk(self) -> None:
        payload = {
            "updated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
            "reports": self.reports,
        }
        with open(self.path, "w", encoding="utf-8") as file:
            json.dump(payload, file, ensure_ascii=False, indent=2)

    def update_report(
        self,
        new_report: str,
        language: str = "fr",
        report_type: str = "hotspot",
        recommendation: str = "",
    ) -> None:
        language = self._normalize_language(language)
        report_type = self._normalize_report_type(report_type)
        with self.lock:
            self.reports[report_type][language] = new_report
            reco_key = "reco_fr" if language == "fr" else "reco_en"
            if recommendation:
                self.reports[report_type][reco_key] = recommendation
            self._write_to_disk()

    def update_reports(
        self,
        fr_report: str,
        en_report: str,
        report_type: str = "hotspot",
        reco_fr: str = "",
        reco_en: str = "",
    ) -> None:
        report_type = self._normalize_report_type(report_type)
        with self.lock:
            self.reports[report_type]["fr"] = fr_report
            self.reports[report_type]["en"] = en_report
            self.reports[report_type]["reco_fr"] = reco_fr
            self.reports[report_type]["reco_en"] = reco_en
            self._write_to_disk()

    def get_report(self, language: str = "fr", report_type: str = "hotspot") -> str:
        language = self._normalize_language(language)
        report_type = self._normalize_report_type(report_type)
        with self.lock:
            return self.reports[report_type].get(language, self.reports[report_type]["fr"])

    def get_reports(self, report_type: str = "hotspot") -> dict:
        report_type = self._normalize_report_type(report_type)
        with self.lock:
            return dict(self.reports[report_type])

    def get_all_reports(self) -> dict:
        with self.lock:
            return {
                "hotspot": dict(self.reports["hotspot"]),
                "weak_signal": dict(self.reports["weak_signal"]),
            }


# Module-level singleton instance and accessor helpers to avoid tight coupling.
_last_weekly_reports_instance = LastWeeklyReportStore()


def get_last_weekly_report(language: str = "fr", report_type: str = "hotspot") -> str:
    """Return the last generated report by `language` (fr|en) and `report_type` (hotspot|weak_signal)."""
    return _last_weekly_reports_instance.get_report(language=language, report_type=report_type)


def get_last_weekly_reports(report_type: str = "hotspot") -> dict:
    """Return both languages for one report type."""
    return _last_weekly_reports_instance.get_reports(report_type=report_type)


def get_all_last_weekly_reports() -> dict:
    """Return all reports: hotspot/weak_signal in fr/en."""
    return _last_weekly_reports_instance.get_all_reports()


def update_last_weekly_report(new_report: str, language: str = "fr", report_type: str = "hotspot") -> None:
    """Update one report using `language` and `report_type`."""
    _last_weekly_reports_instance.update_report(new_report, language=language, report_type=report_type)


def update_last_weekly_reports(
    fr_report: str,
    en_report: str,
    report_type: str = "hotspot",
    reco_fr: str = "",
    reco_en: str = "",
) -> None:
    """Update both FR/EN reports for one report type."""
    _last_weekly_reports_instance.update_reports(
        fr_report=fr_report,
        en_report=en_report,
        report_type=report_type,
        reco_fr=reco_fr,
        reco_en=reco_en,
    )


def get_last_hotspot_report() -> str:
    """Return the last generated hotspot report in French (legacy accessor)."""
    return get_last_weekly_report(language="fr", report_type="hotspot")


def get_last_hotspot_report_by_language(language: str) -> str:
    """Return the last generated hotspot report in the requested language (`fr` or `en`)."""
    return get_last_weekly_report(language=language, report_type="hotspot")


def get_last_hotspot_reports() -> dict:
    """Return both FR and EN hotspot reports."""
    return get_last_weekly_reports(report_type="hotspot")


def update_last_hotspot_report(new_report: str, language: str = "fr") -> None:
    """Update and persist a single-language hotspot report (`fr` by default)."""
    update_last_weekly_report(new_report, language=language, report_type="hotspot")


def update_last_hotspot_reports(fr_report: str, en_report: str) -> None:
    """Update and persist hotspot reports for both languages."""
    update_last_weekly_reports(fr_report=fr_report, en_report=en_report, report_type="hotspot")


def _build_hotspot_prompt(today: str, no_data_message_en: str, no_data_message_fr: str) -> str:
    return f"""
    You are the Lead Data Analyst for Montreal Mobility.
    Generate the WEEKLY MOBILITY HOTSPOTS BRIEFING based on the last 7 days. Today is {today}.

    TASK:
    1. Query the database to find data for the LAST 7 DAYS. 
    2. Identify the TOP 5 absolute worst hotspots based on the highest concentration of collisions or 311 requests.

   "🚨 GEOGRAPHY & SQL RULES (CRITICAL) 🚨\n"
    "- NEVER group by or display raw X/Y coordinates, Lat/Long, or points.\n"
    "- For collisions, group by 'ACCDN_PRES_DE' or 'RUE_ACCDN'.\n"
    "- For 311 requests, DO NOT just group by Borough. You MUST group by Borough AND the specific issue topic (e.g., snow removal, potholes, lighting) to find the real pain points.\n"
    "- SQL TIP FOR 311: `GROUP BY ARRONDISSEMENT, NATURE` (or whichever column contains the specific issue like 'déneigement').\n\n"

    "CRITICAL ANTI-HALLUCINATION RULES:\n"
    "- You MUST rely EXCLUSIVELY on the data retrieved by your tools.\n"
    "- DO NOT invent weather, times, or durations like '7 derniers jours' just to fill space. If the SQL query didn't return a specific time or weather, just don't mention it.\n"
    f"- If there is no data, `english_report` must be exactly: \"{no_data_message_en}\"\n"
    f"- If there is no data, `french_report` must be exactly: \"{no_data_message_fr}\"\n\n"

    "STRICT FORMATTING RULES (Only for REAL data):\n"
    "- You MUST use exactly this format for the output:\n"
    "  \"Hotspot #[Rank] : [Intersection/Zone name] - [Total Number] [SPECIFIC Type of Issue] ([Any real details from DB]), [Weather/Time IF retrieved].\"\n"
    "- Example 1 (Collision): \"Hotspot #1 : Intersection Peel/Ste-Catherine - 12 collisions (dont 3 graves), surtout entre 16h-19h, sous la pluie.\"\n"
    "- Example 2 (311): \"Hotspot #2 : Arrondissement Mercier - 145 requêtes 311 (Déneigement).\"\n"
    "- DO NOT output generic lists like '(Requête, Plainte, Commentaire)'. We want to know the EXACT dominant issue driving the hotspot.\n"
    - ABSOLUTELY NO GPS COORDINATES IN THE TEXT.

    RECOMMENDATIONS SECTION:
    - Provide 2 to 4 short, actionable, and preventive recommendations tied to the hotspots.

    OUTPUT FORMAT (CRITICAL):
    You MUST output ONLY a valid JSON object. No markdown formatting, no ```json fences:
    {{
        "english_report": "...",
        "french_report": "...",
        "english_recommendations": "...",
        "french_recommendations": "..."
    }}
    """

def _build_weak_signal_prompt(today: str, no_data_message_en: str, no_data_message_fr: str) -> str:
    return f"""
    You are the Lead Data Analyst for Montreal Mobility.
    Generate the WEEKLY MOBILITY WEAK SIGNALS BRIEFING for the last 7 days. Today is {today}.

    BUSINESS RULES:
    - A 'weak signal' is a small early change that may indicate a future problem (e.g., a sudden spike in a specific 311 request in a quiet area).
    - Query the database for the last 7 days using SQLite syntax: `WHERE date_column >= date('{today}', '-7 days')`.
    - Compare this to historical averages if necessary.
    - If there are no weak signals, `english_report` must be exactly: "{no_data_message_en}"
    - If there are no weak signals, `french_report` must be exactly: "{no_data_message_fr}"
    - DO NOT INVENT SIGNALS.

    RECOMMENDATIONS SECTION:
    - Provide 2 to 4 short, preventive recommendations based on the weak signals identified.

    OUTPUT FORMAT (CRITICAL):
    You MUST output ONLY a valid JSON object. No markdown formatting, no ```json fences, just the raw JSON:
    {{
        "english_report": "...",
        "french_report": "...",
        "english_recommendations": "...",
        "french_recommendations": "..."
    }}
    """


def _fallback_recommendations(language: str, report_type: str) -> str:
    """Fallback recommendations text only, without section title."""
    fallbacks = {
        "fr": {
            "hotspot": "- Prioriser des interventions ciblées (signalisation, marquage) sur les zones touchées.\n- Renforcer la prévention aux périodes critiques observées.",
            "weak_signal": "- Surveiller de près les zones en hausse pour confirmer la tendance.\n- Déployer des actions préventives légères dès la semaine suivante."
        },
        "en": {
            "hotspot": "- Prioritize targeted interventions (signage, markings) in the most affected areas.\n- Increase prevention during critical time windows.",
            "weak_signal": "- Closely monitor rising zones to confirm or reject the trend.\n- Deploy light preventive actions in the following week."
        }
    }
    return fallbacks.get(language, fallbacks["fr"]).get(report_type, fallbacks["fr"]["hotspot"])

def _extract_bilingual_reports(raw_content: str) -> dict:
    """Extract bilingual payload from model output."""
    if not isinstance(raw_content, str):
        return {}

    content = raw_content.strip()
    if not content:
        return {}

    try:
        payload = json.loads(content)
        if isinstance(payload, dict):
            return payload
    except Exception:
        pass

    start = content.find("{")
    end = content.rfind("}")
    if start >= 0 and end > start:
        try:
            payload = json.loads(content[start : end + 1])
            if isinstance(payload, dict):
                return payload
        except Exception:
            return {}
    return {}


def _generate_weekly_reports_bilingual(langgraph_app, today: str, report_type: str) -> tuple[str, str, str, str]:
    report_type = report_type.lower()

    no_data_messages = {
        "hotspot": {
            "fr": "Aucun incident ou requête majeure n'a été enregistré dans nos bases de données pour cette semaine.",
            "en": "No major incidents or requests were recorded in our databases this week.",
        },
        "weak_signal": {
            "fr": "Aucun signal faible détecté cette semaine.",
            "en": "No weak signals detected this week.",
        },
    }

    no_data_message_en = no_data_messages[report_type]["en"]
    no_data_message_fr = no_data_messages[report_type]["fr"]

    if report_type == "hotspot":
        prompt = _build_hotspot_prompt(
            today=today,
            no_data_message_en=no_data_message_en,
            no_data_message_fr=no_data_message_fr,
        )
    else:
        prompt = _build_weak_signal_prompt(
            today=today,
            no_data_message_en=no_data_message_en,
            no_data_message_fr=no_data_message_fr,
        )

    initial_state = {
        "messages": [HumanMessage(content=prompt)],
        "audience": "grand_public",
        "is_ambiguous": False,
    }

    resultats = langgraph_app.invoke(initial_state)
    raw_content = resultats.get("analytical_response", "")

    payload = _extract_bilingual_reports(raw_content)
    en_report = payload.get("english_report") or payload.get("en") or raw_content
    fr_report = payload.get("french_report") or payload.get("fr")
    en_reco = payload.get("english_recommendations") or payload.get("reco_en")
    fr_reco = payload.get("french_recommendations") or payload.get("reco_fr")

    if not isinstance(en_report, str) or not en_report.strip():
        en_report = "Generation error."
    if not isinstance(fr_report, str) or not fr_report.strip():
        # Fallback minimal si le modèle n'a pas respecté le JSON demandé.
        fr_report = "Erreur de traduction. Version anglaise disponible ci-dessous :\n\n" + en_report

    if not isinstance(en_reco, str):
        en_reco = ""
    if not isinstance(fr_reco, str):
        fr_reco = ""

    if en_report.strip() == no_data_message_en and (not fr_report.strip()):
        fr_report = no_data_message_fr
    if fr_report.strip() == no_data_message_fr and (not en_report.strip()):
        en_report = no_data_message_en

    if en_report.strip() != no_data_message_en and not en_reco.strip():
        _, en_reco = _split_report_and_recommendations_text(en_report, "en")
        if not en_reco.strip():
            en_reco = _fallback_recommendations(language="en", report_type=report_type)
    if fr_report.strip() != no_data_message_fr and not fr_reco.strip():
        _, fr_reco = _split_report_and_recommendations_text(fr_report, "fr")
        if not fr_reco.strip():
            fr_reco = _fallback_recommendations(language="fr", report_type=report_type)

    # Nettoyage éventuel si la section recommandations est restée dans le texte du rapport.
    en_report, extracted_en_reco = _split_report_and_recommendations_text(en_report, "en")
    fr_report, extracted_fr_reco = _split_report_and_recommendations_text(fr_report, "fr")
    if extracted_en_reco and not en_reco.strip():
        en_reco = extracted_en_reco
    if extracted_fr_reco and not fr_reco.strip():
        fr_reco = extracted_fr_reco

    if report_type == "hotspot":
        en_prefix = "Here is the weekly mobility hotspots report for Montreal:"
        fr_prefix = "Voici le rapport hebdomadaire des hotspots de mobilité à Montréal :"
    else:
        en_prefix = "Here is the weekly mobility weak signals report for Montreal:"
        fr_prefix = "Voici le rapport hebdomadaire des signaux faibles de mobilité à Montréal :"

    en_full = f"{en_prefix}\n\n{en_report}"
    fr_full = f"{fr_prefix}\n\n{fr_report}"
    return fr_full, en_full, fr_reco.strip(), en_reco.strip()


def hebdo_hotspots_briefing_generator(language: str = "français"):
    """Générer et persister le briefing hotspots en FR/EN (single JSON)."""
    _ = language  # Kept for backward compatibility with existing call signatures.
    today = time.strftime("%Y-%m-%d")
    try:
        langgraph_app = get_langgraph_app()
        fr_report, en_report, reco_fr, reco_en = _generate_weekly_reports_bilingual(
            langgraph_app,
            today=today,
            report_type="hotspot",
        )
        update_last_weekly_reports(
            fr_report=fr_report,
            en_report=en_report,
            report_type="hotspot",
            reco_fr=reco_fr,
            reco_en=reco_en,
        )

    except Exception as e:
        logger.exception("Erreur lors de la génération du rapport")
        update_last_weekly_reports(
            fr_report=f"Erreur lors de la génération du rapport : {e}",
            en_report=f"Error while generating the report: {e}",
            report_type="hotspot",
            reco_fr="",
            reco_en="",
        )


def hebdo_weak_signals_briefing_generator():
    """Générer et persister le briefing weak signals en FR/EN (single JSON)."""
    today = time.strftime("%Y-%m-%d")
    try:
        langgraph_app = get_langgraph_app()
        fr_report, en_report, reco_fr, reco_en = _generate_weekly_reports_bilingual(
            langgraph_app,
            today=today,
            report_type="weak_signal",
        )
        update_last_weekly_reports(
            fr_report=fr_report,
            en_report=en_report,
            report_type="weak_signal",
            reco_fr=reco_fr,
            reco_en=reco_en,
        )

    except Exception as e:
        logger.exception("Erreur lors de la génération du rapport weak signals")
        update_last_weekly_reports(
            fr_report=f"Erreur lors de la génération des signaux faibles : {e}",
            en_report=f"Error while generating weak signals report: {e}",
            report_type="weak_signal",
            reco_fr="",
            reco_en="",
        )