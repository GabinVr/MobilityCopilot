from fastapi import APIRouter, HTTPException, Query
from services.weekly_report import generate_weekly_report, get_last_weekly_report

weekly_report_router = APIRouter()


def _normalize_language(language: str) -> str:
    return "en" if (language or "").strip().lower().startswith("en") else "fr"

@weekly_report_router.get("/last_weekly_report")
def last_weekly_report_endpoint(
    language: str = Query("fr")
):
    """Return the latest generated weekly mobility report for Montreal.
    Supports query parameters:
    - language: fr|en
    """
    normalized_language = _normalize_language(language)
    report = get_last_weekly_report(normalized_language)
    if not report or not report.get("path"):
        report = generate_weekly_report(normalized_language)

    if not report or not report.get("path"):
        raise HTTPException(status_code=404, detail="Weekly report not available")

    return {
        "language": normalized_language,
        "pdf_path": report.get("path"),
        "generated_at": report.get("generated_at"),
        "sections": report.get("sections", ["hotspot", "weak_signal"]),
    }