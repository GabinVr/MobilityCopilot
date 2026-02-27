from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import FileResponse
import os
from services.weekly_report import generate_weekly_report, get_last_weekly_report

weekly_report_router = APIRouter()


def _normalize_language(language: str) -> str:
    return "en" if (language or "").strip().lower().startswith("en") else "fr"

@weekly_report_router.get("/last_weekly_report")
def last_weekly_report_endpoint(language: str = Query("fr")):
    """Download the latest generated weekly mobility report PDF.
    
    Supports query parameters:
    - language: fr|en
    
    Returns: PDF file for download
    """
    normalized_language = _normalize_language(language)
    report = get_last_weekly_report(normalized_language)
    
    if not report or not report.get("path"):
        report = generate_weekly_report(normalized_language)

    if not report or not report.get("path"):
        raise HTTPException(status_code=404, detail="Weekly report not available")
    
    file_path = report.get("path")
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="Report file not found on server")
    
    filename = f"weekly_report_{normalized_language}.pdf"
    return FileResponse(
        path=file_path,
        filename=filename,
        media_type="application/pdf"
    )