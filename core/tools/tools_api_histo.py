from __future__ import annotations

import datetime as dt
import calendar
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import requests
from pydantic import BaseModel, Field
from langchain_core.tools import StructuredTool


# =========================
# Constants
# =========================

GEOMET_BASE = "https://api.weather.gc.ca"
CLIMATE_DAILY_ITEMS = f"{GEOMET_BASE}/collections/climate-daily/items"
CLIMATE_MONTHLY_ITEMS = f"{GEOMET_BASE}/collections/climate-monthly/items"

UA = "MobilityCopilot/1.0 (mtl-global-history; geomet)"
TIMEOUT_S = 25

MAX_PERIODS = 15

MTL_REFERENCE = {
    "name": "Montreal (global proxy via reference station)",
    "label": "Montreal/Trudeau International",
    "climate_identifier": "7025251",
}

FREQ_ALIASES = {
    "day": "day",
    "daily": "day",
    "jour": "day",
    "jours": "day",
    "week": "week",
    "weekly": "week",
    "semaine": "week",
    "mois": "month",
    "month": "month",
    "monthly": "month",
    "annee": "year",
    "année": "year",
    "year": "year",
    "yearly": "year",
}


# =========================
# Input schema (no period_max)
# =========================

class MTLHistoryGlobalInput(BaseModel):
    start_date: Optional[str] = Field(default=None, description="YYYY-MM-DD")
    end_date: Optional[str] = Field(default=None, description="YYYY-MM-DD (defaults to today UTC)")
    frequency: str = Field(default="month", description="day | week | month | year (also: jour(s) | semaine | mois | annee)")


# =========================
# Helpers
# =========================

def _utc_now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()

def _parse_date(s: str) -> dt.date:
    return dt.date.fromisoformat(s)

def _to_float(v: Any) -> Optional[float]:
    if v is None or isinstance(v, bool):
        return None
    try:
        return float(v)
    except Exception:
        return None

def _normalize_freq(freq: str) -> str:
    f = (freq or "").strip().lower()
    if f not in FREQ_ALIASES:
        raise ValueError(f"Invalid frequency '{freq}'. Use day|week|month|year (or jour(s)|semaine|mois|annee).")
    return FREQ_ALIASES[f]

def _follow_next_link(payload: Dict[str, Any]) -> Optional[str]:
    for link in payload.get("links", []) or []:
        if isinstance(link, dict) and link.get("rel") == "next":
            href = link.get("href")
            if href:
                return href
    return None

def _http_get_json(url: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    r = requests.get(
        url,
        params=params,
        timeout=TIMEOUT_S,
        headers={"User-Agent": UA, "Accept": "application/geo+json,application/json;q=0.9,*/*;q=0.1"},
    )
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, dict):
        raise ValueError(f"Expected JSON object from {url}, got {type(data)}")
    return data

def _floor_period_start(d: dt.date, freq: str) -> dt.date:
    if freq == "day":
        return d
    if freq == "week":
        return d - dt.timedelta(days=d.weekday())
    if freq == "month":
        return dt.date(d.year, d.month, 1)
    if freq == "year":
        return dt.date(d.year, 1, 1)
    raise ValueError("Unsupported frequency")

def _shift_period_start(d: dt.date, freq: str, n: int) -> dt.date:
    if n == 0:
        return d
    if freq == "day":
        return d + dt.timedelta(days=n)
    if freq == "week":
        return d + dt.timedelta(days=7 * n)
    if freq == "month":
        y, m = d.year, d.month + n
        while m <= 0:
            m += 12
            y -= 1
        while m > 12:
            m -= 12
            y += 1
        return dt.date(y, m, 1)
    if freq == "year":
        return dt.date(d.year + n, 1, 1)
    raise ValueError("Unsupported frequency")

def _period_id(pstart: dt.date, freq: str) -> str:
    if freq == "day":
        return pstart.isoformat()
    if freq == "week":
        iy, iw, _ = pstart.isocalendar()
        return f"{iy}-W{iw:02d}"
    if freq == "month":
        return f"{pstart.year:04d}-{pstart.month:02d}"
    if freq == "year":
        return f"{pstart.year:04d}"
    raise ValueError("Unsupported frequency")

def _bucket_end(pstart: dt.date, freq: str) -> dt.date:
    if freq == "day":
        return pstart
    if freq == "week":
        return pstart + dt.timedelta(days=6)
    if freq == "month":
        nxt = _shift_period_start(pstart, "month", 1)
        return nxt - dt.timedelta(days=1)
    if freq == "year":
        return dt.date(pstart.year, 12, 31)
    raise ValueError("Unsupported frequency")

def _clamp(d: dt.date, lo: dt.date, hi: dt.date) -> dt.date:
    return lo if d < lo else hi if d > hi else d

def _round_floats(obj: Any, ndigits: int = 2) -> Any:
    if isinstance(obj, bool):
        return obj
    if isinstance(obj, float):
        rounded = round(obj, ndigits)
        return 0.0 if rounded == 0 else rounded
    if isinstance(obj, dict):
        return {k: _round_floats(v, ndigits) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_round_floats(v, ndigits) for v in obj]
    return obj

def _infer_effective_start(requested_start: Optional[dt.date], end: dt.date, freq: str) -> tuple[dt.date, bool]:
    """
    Hard-truncate to MAX_PERIODS ending at 'end'.
    Returns (effective_start, truncated_flag).
    """
    anchor = _floor_period_start(end, freq)
    earliest = _shift_period_start(anchor, freq, -(MAX_PERIODS - 1))
    if requested_start is None:
        return earliest, True
    if requested_start >= earliest:
        return requested_start, False
    return earliest, True


# =========================
# Fetch climate-daily / climate-monthly
# =========================

def _fetch_features_by_years(items_url: str, climate_id: str, y0: int, y1: int, limit: int = 1000) -> List[Dict[str, Any]]:
    cql = (
        f"properties.CLIMATE_IDENTIFIER = '{climate_id}' "
        f"AND properties.LOCAL_YEAR >= {int(y0)} "
        f"AND properties.LOCAL_YEAR <= {int(y1)}"
    )
    params = {"f": "json", "lang": "en", "limit": max(1, min(int(limit), 10000)), "filter": cql}

    feats: List[Dict[str, Any]] = []
    url = items_url
    page = 0
    while True:
        payload = _http_get_json(url, params=params if page == 0 else None)
        page += 1
        feats.extend(payload.get("features", []) or [])
        nxt = _follow_next_link(payload)
        if not nxt:
            break
        url = nxt
        params = None
        if page > 200:
            break
    return feats


def _parse_daily(feat: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    props = (feat or {}).get("properties") or {}
    if not isinstance(props, dict):
        return None

    local_date = props.get("LOCAL_DATE")
    d: Optional[dt.date] = None
    if isinstance(local_date, str):
        try:
            d = dt.date.fromisoformat(local_date[:10])
        except Exception:
            d = None
    if d is None:
        try:
            d = dt.date(int(props["LOCAL_YEAR"]), int(props["LOCAL_MONTH"]), int(props["LOCAL_DAY"]))
        except Exception:
            return None

    return {
        "date": d,
        "mean_temp_c": _to_float(props.get("MEAN_TEMPERATURE")),
        "min_temp_c": _to_float(props.get("MIN_TEMPERATURE")),
        "max_temp_c": _to_float(props.get("MAX_TEMPERATURE")),
        "total_precip": _to_float(props.get("TOTAL_PRECIPITATION")),
        "total_snow": _to_float(props.get("TOTAL_SNOW")),
    }


def _parse_monthly(feat: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    props = (feat or {}).get("properties") or {}
    if not isinstance(props, dict):
        return None
    try:
        y = int(props["LOCAL_YEAR"])
        m = int(props["LOCAL_MONTH"])
    except Exception:
        return None

    return {
        "year": y,
        "month": m,
        "mean_temp_c": _to_float(props.get("MEAN_TEMPERATURE")),
        "min_temp_c": _to_float(props.get("MIN_TEMPERATURE")),
        "max_temp_c": _to_float(props.get("MAX_TEMPERATURE")),
        "total_precip": _to_float(props.get("TOTAL_PRECIPITATION")),
        "total_snow": _to_float(props.get("TOTAL_SNOWFALL")),
    }


# =========================
# Aggregation
# =========================

@dataclass
class Bucket:
    pstart: dt.date
    pend: dt.date
    mean_sum: float = 0.0
    mean_n: int = 0
    min_min: Optional[float] = None
    max_max: Optional[float] = None
    precip_sum: float = 0.0
    precip_n: int = 0
    snow_sum: float = 0.0
    snow_n: int = 0


def _agg_from_daily(dailies: List[Dict[str, Any]], start: dt.date, end: dt.date, freq: str) -> List[Dict[str, Any]]:
    buckets: Dict[str, Bucket] = {}
    for r in dailies:
        d = r["date"]
        if d < start or d > end:
            continue
        pstart = _floor_period_start(d, freq)
        pid = _period_id(pstart, freq)
        if pid not in buckets:
            buckets[pid] = Bucket(pstart=pstart, pend=_bucket_end(pstart, freq))

        b = buckets[pid]

        mt = r.get("mean_temp_c")
        if isinstance(mt, (int, float)):
            b.mean_sum += float(mt)
            b.mean_n += 1

        mn = r.get("min_temp_c")
        if isinstance(mn, (int, float)):
            b.min_min = float(mn) if b.min_min is None else min(b.min_min, float(mn))

        mx = r.get("max_temp_c")
        if isinstance(mx, (int, float)):
            b.max_max = float(mx) if b.max_max is None else max(b.max_max, float(mx))

        pr = r.get("total_precip")
        if isinstance(pr, (int, float)):
            b.precip_sum += float(pr)
            b.precip_n += 1

        sn = r.get("total_snow")
        if isinstance(sn, (int, float)):
            b.snow_sum += float(sn)
            b.snow_n += 1

    out = []
    for pid, b in sorted(buckets.items(), key=lambda kv: kv[1].pstart):
        out.append({
            "period_id": pid,
            "start_date": _clamp(b.pstart, start, end).isoformat(),
            "end_date": _clamp(b.pend, start, end).isoformat(),
            "mean_temp_c": (b.mean_sum / b.mean_n) if b.mean_n else None,
            "min_temp_c": b.min_min,
            "max_temp_c": b.max_max,
            "total_precip": b.precip_sum if b.precip_n else None,
            "total_snow": b.snow_sum if b.snow_n else None,
        })
    return out


def _agg_from_monthly_to_month(months: List[Dict[str, Any]], start: dt.date, end: dt.date) -> List[Dict[str, Any]]:
    out = []
    for r in sorted(months, key=lambda x: (x["year"], x["month"])):
        pstart = dt.date(r["year"], r["month"], 1)
        pend = dt.date(r["year"], r["month"], calendar.monthrange(r["year"], r["month"])[1])
        if pend < start or pstart > end:
            continue
        pid = f"{r['year']:04d}-{r['month']:02d}"
        out.append({
            "period_id": pid,
            "start_date": _clamp(pstart, start, end).isoformat(),
            "end_date": _clamp(pend, start, end).isoformat(),
            "mean_temp_c": r.get("mean_temp_c"),
            "min_temp_c": r.get("min_temp_c"),
            "max_temp_c": r.get("max_temp_c"),
            "total_precip": r.get("total_precip"),
            "total_snow": r.get("total_snow"),
        })
    return out


def _agg_from_monthly_to_year(months: List[Dict[str, Any]], start: dt.date, end: dt.date) -> List[Dict[str, Any]]:
    buckets: Dict[int, Bucket] = {}
    for r in months:
        pstart = dt.date(r["year"], r["month"], 1)
        pend = dt.date(r["year"], r["month"], calendar.monthrange(r["year"], r["month"])[1])
        if pend < start or pstart > end:
            continue

        y = r["year"]
        if y not in buckets:
            buckets[y] = Bucket(pstart=dt.date(y, 1, 1), pend=dt.date(y, 12, 31))

        b = buckets[y]

        mt = r.get("mean_temp_c")
        if isinstance(mt, (int, float)):
            days = calendar.monthrange(r["year"], r["month"])[1]
            b.mean_sum += float(mt) * days
            b.mean_n += days

        mn = r.get("min_temp_c")
        if isinstance(mn, (int, float)):
            b.min_min = float(mn) if b.min_min is None else min(b.min_min, float(mn))

        mx = r.get("max_temp_c")
        if isinstance(mx, (int, float)):
            b.max_max = float(mx) if b.max_max is None else max(b.max_max, float(mx))

        pr = r.get("total_precip")
        if isinstance(pr, (int, float)):
            b.precip_sum += float(pr)
            b.precip_n += 1

        sn = r.get("total_snow")
        if isinstance(sn, (int, float)):
            b.snow_sum += float(sn)
            b.snow_n += 1

    out = []
    for y, b in sorted(buckets.items(), key=lambda kv: kv[0]):
        out.append({
            "period_id": f"{y:04d}",
            "start_date": _clamp(b.pstart, start, end).isoformat(),
            "end_date": _clamp(b.pend, start, end).isoformat(),
            "mean_temp_c": (b.mean_sum / b.mean_n) if b.mean_n else None,
            "min_temp_c": b.min_min,
            "max_temp_c": b.max_max,
            "total_precip": b.precip_sum if b.precip_n else None,
            "total_snow": b.snow_sum if b.snow_n else None,
        })
    return out


# =========================
# Tool logic
# =========================

def geomet_mtl_history_global(start_date: Optional[str] = None, end_date: Optional[str] = None, frequency: str = "month") -> Dict[str, Any]:
    meta = {
        "tool": "geomet_mtl_history_global",
        "version": "1.0",
        "generated_at_utc": _utc_now_iso(),
        "language": "en",
        "max_periods": MAX_PERIODS,
    }

    try:
        freq = _normalize_freq(frequency)

        end = _parse_date(end_date) if end_date else dt.datetime.now(dt.timezone.utc).date()
        req_start = _parse_date(start_date) if start_date else None
        if req_start and req_start > end:
            raise ValueError("start_date must be <= end_date")

        eff_start, truncated = _infer_effective_start(req_start, end, freq)

        climate_id = MTL_REFERENCE["climate_identifier"]

        if freq in ("day", "week"):
            feats = _fetch_features_by_years(CLIMATE_DAILY_ITEMS, climate_id, eff_start.year, end.year, limit=1000)
            dailies = [r for r in (_parse_daily(f) for f in feats) if r is not None]
            periods = _agg_from_daily(dailies, eff_start, end, freq)

        else:
            feats = _fetch_features_by_years(CLIMATE_MONTHLY_ITEMS, climate_id, eff_start.year, end.year, limit=1000)
            months = [r for r in (_parse_monthly(f) for f in feats) if r is not None]

            if freq == "month":
                periods = _agg_from_monthly_to_month(months, eff_start, end)
            else:
                periods = _agg_from_monthly_to_year(months, eff_start, end)

        if len(periods) > MAX_PERIODS:
            periods = periods[-MAX_PERIODS:]
            truncated = True

        summary = (
            f"Montreal historical ({freq}) from {eff_start.isoformat()} to {end.isoformat()} "
            f"({len(periods)} periods, max {MAX_PERIODS})."
        )

        payload = {
            "meta": meta,
            "summary": summary,
            "query": {
                "requested_start_date": req_start.isoformat() if req_start else None,
                "requested_end_date": end.isoformat(),
                "effective_start_date": eff_start.isoformat(),
                "effective_end_date": end.isoformat(),
                "frequency": freq,
                "truncated_to_max_periods": truncated,
            },
            "montreal_global": {
                "periods": periods,
                "units": {
                    "temperature": "C",
                    "total_precip": "dataset units (climate-daily/monthly)",
                    "total_snow": "dataset units (climate-daily/monthly)",
                },
            },
            "sources": {
                "climate_daily_items": CLIMATE_DAILY_ITEMS,
                "climate_monthly_items": CLIMATE_MONTHLY_ITEMS,
                "reference_station": {
                    "climate_identifier": climate_id,
                    "label": MTL_REFERENCE["label"],
                },
            },
        }

        return _round_floats(payload, ndigits=2)

    except requests.HTTPError as e:
        return {"meta": meta, "error": {"type": "HTTPError", "message": str(e)}}
    except Exception as e:
        return {"meta": meta, "error": {"type": type(e).__name__, "Tmessage": str(e)}}


geomet_mtl_history_global_tool = StructuredTool.from_function(
    func=geomet_mtl_history_global,
    name="geomet_mtl_history_global",
    description=(
        "HISTORICAL WEATHER TOOL. Use this tool ONLY to get past weather data for Montreal "
        "(temperatures, rain, snow). DO NOT USE IT for current weather or future forecasts.\n"
        "- start_date and end_date MUST be in the exact 'YYYY-MM-DD' format (e.g., '2019-01-15').\n"
        "- frequency: choose from 'day', 'week', 'month', or 'year'.\n"
        "WARNING: You can only retrieve up to 50 aggregated periods at once. If the requested date range exceeds this limit, the data will be truncated to the most recent 50 periods."
        "If you need the weather for a single specific day, set both start_date and end_date to that exact same date and use frequency='day'."
    ),
    args_schema=MTLHistoryGlobalInput,
)

if __name__ == "__main__":
    import json
    print(json.dumps(geomet_mtl_history_global_tool.invoke({
        "start_date": "1900-01-01",
        "end_date": "2015-02-24",
        "frequency": "year",
    }), indent=2, ensure_ascii=False))