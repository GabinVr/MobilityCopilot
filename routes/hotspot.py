from fastapi import APIRouter, HTTPException, Query
from services.weekly_report import (
    get_all_last_weekly_reports,
    hebdo_hotspots_briefing_generator,
    hebdo_weak_signals_briefing_generator,
    get_last_weekly_report,
)

hotspot_router = APIRouter()


def _normalize_language(language: str) -> str:
    return "en" if (language or "").strip().lower().startswith("en") else "fr"


def _normalize_report_type(report_type: str) -> str:
    value = (report_type or "hotspot").strip().lower().replace("-", "_")
    return "weak_signal" if value in {"weak_signal", "weak_signals", "weaksignal"} else "hotspot"


def _maybe_generate_if_empty(language: str, report_type: str, report: str) -> str:
    empty_hotspot = {
        "Aucun rapport généré pour le moment.",
        "No report has been generated yet.",
    }
    empty_weak_signal = {
        "Aucun signal faible généré pour le moment.",
        "No weak signal report has been generated yet.",
    }

    if report_type == "weak_signal" and report in empty_weak_signal:
        hebdo_weak_signals_briefing_generator()
        return get_last_weekly_report(language=language, report_type=report_type)

    if report_type == "hotspot" and report in empty_hotspot:
        hebdo_hotspots_briefing_generator()
        return get_last_weekly_report(language=language, report_type=report_type)

    return report


@hotspot_router.post("/generate_weekly_reports")
def generate_all_weekly_reports_endpoint():
    """Generate all weekly reports in one call: hotspot + weak_signal, FR + EN."""
    try:
        hebdo_hotspots_briefing_generator()
        hebdo_weak_signals_briefing_generator()

        return {
            "status": "ok",
            "generated_types": ["hotspot", "weak_signal"],
            "reports": get_all_last_weekly_reports(),
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Generation failed: {exc}") from exc


@hotspot_router.get("/last_weekly_report")
def last_weekly_report_endpoint(
    language: str = Query("fr"),
    report_type: str = Query("hotspot", alias="type"),
):
    """Return the latest generated weekly mobility report for Montreal.

    Supports query parameters:
    - language: fr|en
    - type: hotspot|weak_signal

    If no report has been generated yet for the requested type/language,
    launch the matching generator and return the result.
    """
    normalized_language = _normalize_language(language)
    normalized_report_type = _normalize_report_type(report_type)

    report = get_last_weekly_report(language=normalized_language, report_type=normalized_report_type)
    report = _maybe_generate_if_empty(normalized_language, normalized_report_type, report)

    return {
        "type": normalized_report_type,
        "language": normalized_language,
        "report": report,
    }