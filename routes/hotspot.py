from fastapi import APIRouter, HTTPException, Query
from services.weekly_report import (
    get_all_last_weekly_reports,
    hebdo_hotspots_briefing_generator,
    hebdo_weak_signals_briefing_generator,
    get_last_weekly_report,
)

hotspot_router = APIRouter()


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
@hotspot_router.get("/last_hotspot_report")
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
    report = get_last_weekly_report(language=language, report_type=report_type)

    report_type_normalized = report_type.strip().lower().replace("-", "_")
    is_weak_signal = report_type_normalized in {"weak_signal", "weak_signals", "weaksignal"}

    if is_weak_signal:
        if report in {
            "Aucun signal faible généré pour le moment.",
            "No weak signal report has been generated yet.",
        }:
            hebdo_weak_signals_briefing_generator()
            report = get_last_weekly_report(language=language, report_type="weak_signal")
    else:
        if report in {
            "Aucun rapport généré pour le moment.",
            "No report has been generated yet.",
        }:
            hebdo_hotspots_briefing_generator()
            report = get_last_weekly_report(language=language, report_type="hotspot")

    return {
        "type": "weak_signal" if is_weak_signal else "hotspot",
        "language": "en" if language.strip().lower().startswith("en") else "fr",
        "report": report,
    }