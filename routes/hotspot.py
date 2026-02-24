from fastapi import APIRouter
from services.weekly_report import (
    hebdo_hotspots_briefing_generator,
    get_last_hotspot_report,
)

hotspot_router = APIRouter()


@hotspot_router.get("/last_hotspot_report")
def last_hotspot_report_endpoint():
    """Return the latest generated mobility hotspot report for Montreal.

    If no report has been generated yet, launch the generator and return the result.
    """
    report = get_last_hotspot_report()
    if report == "Aucun rapport généré pour le moment.":
        hebdo_hotspots_briefing_generator()
        report = get_last_hotspot_report()
    return {"report": report}