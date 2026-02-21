from __future__ import annotations

import datetime as dt
import math
from typing import Any, Dict, List, Optional, Tuple

import requests
from pydantic import BaseModel
from langchain_core.tools import StructuredTool


CITYPAGE_MTL_ITEM_ID = "qc-147"
CITYPAGE_ENDPOINT = f"https://api.weather.gc.ca/collections/citypageweather-realtime/items/{CITYPAGE_MTL_ITEM_ID}"
SWOB_ITEMS_ENDPOINT = "https://api.weather.gc.ca/collections/swob-realtime/items"

MTL_BBOX: Tuple[float, float, float, float] = (-74.35, 45.35, -73.35, 45.80)
TIMEOUT_S = 20
UA = "MobilityCopilot/0.1 (GeoMet text bundle)"

SWOB_PROPERTIES = [
    "date_tm-value",
    "msc_id-value",
    "icao_stn_id-value",
    "clim_id-value",
    "stn_nam-value",
    "air_temp",
    "air_temp-uom",
    "vis",
    "vis-uom",
    "pcpn_amt_pst1hr",
    "pcpn_amt_pst1hr-uom",
    "rnfl_amt_pst1hr",
    "rnfl_amt_pst1hr-uom",
]

FIXED_MTL_STATION_PROFILES = [
    {
        "key": "downtown",
        "label": "Downtown / McGill (McTavish)",
        "preferred_msc_ids": ["7024745"],
        "preferred_icao_ids": [],
        "name_contains": ["MCTAVISH"],
        "anchor": (45.504926, -73.579185),
    },
    {
        "key": "airport_trudeau",
        "label": "Airport (Trudeau)",
        "preferred_msc_ids": ["7025251", "702S006"],
        "preferred_icao_ids": ["CYUL"],
        "name_contains": ["TRUDEAU", "PIERRE ELLIOTT"],
        "anchor": (45.4705, -73.7409),
    },
    {
        "key": "south_shore",
        "label": "South Shore (St-Hubert)",
        "preferred_msc_ids": ["7027329"],
        "preferred_icao_ids": ["CYHU"],
        "name_contains": ["ST-HUBERT", "HUBERT"],
        "anchor": (45.5181, -73.4169),
    },
    {
        "key": "north",
        "label": "North (Mirabel)",
        "preferred_msc_ids": ["7034900"],
        "preferred_icao_ids": ["CYMX"],
        "name_contains": ["MIRABEL"],
        "anchor": (45.6804, -74.0387),
    },
    {
        "key": "west_island",
        "label": "West Island (Ste-Anne-de-Bellevue)",
        "preferred_msc_ids": ["702FHL8"],
        "preferred_icao_ids": [],
        "name_contains": ["STE-ANNE", "BELLEVUE"],
        "anchor": (45.4270, -73.92892),
    },
]


# =========================
# Helpers
# =========================

def _utc_now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()

def _requests_get(url: str, params: Dict[str, Any]) -> Dict[str, Any]:
    r = requests.get(
        url,
        params=params,
        timeout=TIMEOUT_S,
        headers={"User-Agent": UA, "Accept": "application/json"},
    )
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, dict):
        raise ValueError(f"Expected JSON object from {url}, got {type(data)}")
    return data

def _pick_lang(x: Any, lang: str = "en") -> Any:
    if isinstance(x, dict):
        if lang in x:
            return x[lang]
        if "en" in x:
            return x["en"]
        if "fr" in x:
            return x["fr"]
    return x

def _as_list(x: Any) -> List[Any]:
    if x is None:
        return []
    return x if isinstance(x, list) else [x]

def _safe_float(x: Any) -> Optional[float]:
    if x is None or isinstance(x, bool):
        return None
    try:
        return float(x)
    except Exception:
        return None

def _norm(s: Optional[str]) -> str:
    return (s or "").strip().upper()

def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))

def _fmt(val: Any, unit: str = "", digits: int = 0) -> str:
    if val is None:
        return "N/A"
    f = _safe_float(val)
    if f is None:
        return "N/A"
    if digits <= 0:
        return f"{f:.0f}{unit}"
    return f"{f:.{digits}f}{unit}"


# =========================
# CityPage -> text
# =========================

def _city_now_text_from_citypage(props: Dict[str, Any], lang: str = "en") -> str:
    cc = props.get("currentConditions") or {}

    temp = _safe_float(_pick_lang(((cc.get("temperature") or {}).get("value")), lang))
    temp_unit = _pick_lang(((cc.get("temperature") or {}).get("units")), lang) or "C"

    cond = _pick_lang(cc.get("condition"), lang)

    wc = _safe_float(_pick_lang(((cc.get("windChill") or {}).get("value")), lang))
    rh = _safe_float(_pick_lang(((cc.get("relativeHumidity") or {}).get("value")), lang))

    wind = cc.get("wind") or {}
    w_dir = _pick_lang(((wind.get("direction") or {}).get("value")), lang)
    w_spd = _safe_float(_pick_lang(((wind.get("speed") or {}).get("value")), lang))
    w_gst = _safe_float(_pick_lang(((wind.get("gust") or {}).get("value")), lang))
    w_unit = _pick_lang(((wind.get("speed") or {}).get("units")), lang) or "km/h"

    parts = []
    if temp is not None:
        parts.append(f"{temp:.1f}°{temp_unit}")
    if isinstance(cond, str) and cond:
        parts.append(cond)
    if isinstance(w_dir, str) and w_dir and w_spd is not None:
        parts.append(f"wind {w_dir} {w_spd:.0f} {w_unit}")
    if w_gst is not None and w_gst > 0:
        parts.append(f"gust {w_gst:.0f} {w_unit}")
    if wc is not None:
        parts.append(f"wind chill {wc:.0f}°C")
    if rh is not None:
        parts.append(f"RH {rh:.0f}%")

    observed = _pick_lang(cc.get("timestamp"), lang)
    if isinstance(observed, str) and observed:
        parts.append(f"observed {observed}")

    if not parts:
        return "Montreal now: N/A."
    return "Montreal now: " + ", ".join(parts) + "."

def _forecast_texts_from_citypage(props: Dict[str, Any], n_periods: int = 4, lang: str = "en") -> List[Dict[str, Any]]:
    fg = props.get("forecastGroup") or {}
    forecasts = fg.get("forecasts") or []
    if not isinstance(forecasts, list):
        return []

    out = []
    for f in forecasts[: max(0, int(n_periods))]:
        if not isinstance(f, dict):
            continue
        period = f.get("period") or {}
        period_name = _pick_lang(period.get("textForecastName"), lang)
        text_summary = _pick_lang(f.get("textSummary"), lang)
        if period_name or text_summary:
            out.append(
                {
                    "period": period_name,
                    "text_summary": text_summary,
                }
            )
    return out

def _warnings_from_citypage(props: Dict[str, Any], lang: str = "en") -> List[Dict[str, Any]]:
    out = []
    for w in _as_list(props.get("warnings")):
        if not isinstance(w, dict):
            continue
        title = _pick_lang(w.get("title"), lang)
        text = _pick_lang(w.get("text"), lang) or _pick_lang(w.get("textSummary"), lang)
        if title or text:
            out.append({"title": title, "text": text})
    return out


# =========================
# SWOB -> fixed stations -> text
# =========================

def fetch_swob_bbox(bbox: Tuple[float, float, float, float], limit: int = 500) -> List[Dict[str, Any]]:
    min_lon, min_lat, max_lon, max_lat = bbox
    params = {
        "f": "json",
        "lang": "en",
        "bbox": f"{min_lon},{min_lat},{max_lon},{max_lat}",
        "limit": max(1, min(int(limit), 10000)),
        "sortby": "-date_tm-value",
        "properties": ",".join(SWOB_PROPERTIES),
    }
    raw = _requests_get(SWOB_ITEMS_ENDPOINT, params=params)
    feats = raw.get("features") or []
    if not isinstance(feats, list):
        feats = []

    out: List[Dict[str, Any]] = []
    for ft in feats:
        if not isinstance(ft, dict):
            continue
        p = ft.get("properties") or {}
        g = ft.get("geometry") or {}
        coords = g.get("coordinates")

        # precip 1h: prefer total precip; fallback rainfall
        precip_val = p.get("pcpn_amt_pst1hr")
        precip_u = p.get("pcpn_amt_pst1hr-uom") or "mm"
        if precip_val is None:
            precip_val = p.get("rnfl_amt_pst1hr")
            precip_u = p.get("rnfl_amt_pst1hr-uom") or precip_u

        out.append(
            {
                "msc_id": p.get("msc_id-value"),
                "icao_id": p.get("icao_stn_id-value"),
                "climate_id": p.get("clim_id-value"),
                "name": p.get("stn_nam-value"),
                "observed_at_utc": p.get("date_tm-value"),
                "location": {
                    "lat": coords[1] if isinstance(coords, list) and len(coords) >= 2 else None,
                    "lon": coords[0] if isinstance(coords, list) and len(coords) >= 2 else None,
                },
                "air_temperature": {"value": p.get("air_temp"), "unit": p.get("air_temp-uom") or "°C"},
                "visibility": {"value": p.get("vis"), "unit": p.get("vis-uom") or "km"},
                "precip_1h": {"value": precip_val, "unit": precip_u},
            }
        )

    return out

def _match_by_name_contains(stations_all: List[Dict[str, Any]], needles: List[str]) -> Optional[Dict[str, Any]]:
    needles_u = [_norm(n) for n in needles if n]
    for s in stations_all:
        name_u = _norm(s.get("name"))
        if any(n in name_u for n in needles_u):
            return s
    return None

def _match_by_nearest(stations_all: List[Dict[str, Any]], anchor: Tuple[float, float]) -> Optional[Dict[str, Any]]:
    a_lat, a_lon = anchor
    best = None
    best_d = 1e18
    for s in stations_all:
        lat = (s.get("location") or {}).get("lat")
        lon = (s.get("location") or {}).get("lon")
        if isinstance(lat, (int, float)) and isinstance(lon, (int, float)):
            d = _haversine_km(a_lat, a_lon, float(lat), float(lon))
            if d < best_d:
                best_d = d
                best = s
    return best

def pick_fixed_stations(stations_all: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    by_msc: Dict[str, Dict[str, Any]] = {}
    by_icao: Dict[str, Dict[str, Any]] = {}
    for s in stations_all:
        m = s.get("msc_id")
        i = s.get("icao_id")
        if m and m not in by_msc:
            by_msc[m] = s
        if i and _norm(i) not in by_icao:
            by_icao[_norm(i)] = s

    picked: List[Dict[str, Any]] = []
    used = set()

    for prof in FIXED_MTL_STATION_PROFILES:
        chosen = None

        for msc in prof.get("preferred_msc_ids", []):
            if msc in by_msc:
                chosen = by_msc[msc]
                break

        if chosen is None:
            for icao in prof.get("preferred_icao_ids", []):
                if _norm(icao) in by_icao:
                    chosen = by_icao[_norm(icao)]
                    break

        if chosen is None and prof.get("name_contains"):
            chosen = _match_by_name_contains(stations_all, prof["name_contains"])

        if chosen is None and prof.get("anchor"):
            chosen = _match_by_nearest(stations_all, prof["anchor"])

        if chosen is None:
            picked.append({"key": prof["key"], "label": prof["label"], "status": "unavailable"})
            continue

        chosen = dict(chosen)
        chosen["key"] = prof["key"]
        chosen["label"] = prof["label"]
        chosen["status"] = "ok"

        dedup_key = chosen.get("msc_id") or chosen.get("icao_id") or chosen.get("name")
        if dedup_key in used and prof.get("anchor"):
            alt = _match_by_nearest(stations_all, prof["anchor"])
            if alt is not None:
                chosen = dict(alt)
                chosen["key"] = prof["key"]
                chosen["label"] = prof["label"]
                chosen["status"] = "ok"
                dedup_key = chosen.get("msc_id") or chosen.get("icao_id") or chosen.get("name")

        used.add(dedup_key)
        picked.append(chosen)

    return picked

def _station_text(s: Dict[str, Any]) -> str:
    if s.get("status") != "ok":
        return f"{s.get('label')}: unavailable."

    t = (s.get("air_temperature") or {}).get("value")
    t_u = (s.get("air_temperature") or {}).get("unit") or "°C"

    p = (s.get("precip_1h") or {}).get("value")
    p_u = (s.get("precip_1h") or {}).get("unit") or "mm"

    v = (s.get("visibility") or {}).get("value")
    v_u = (s.get("visibility") or {}).get("unit") or "km"

    obs = s.get("observed_at_utc") or "N/A"

    return (
        f"{s.get('label')}: "
        f"temp {_fmt(t, t_u, digits=1)}, precip_1h {_fmt(p, ' ' + p_u)}, "
        f"vis {_fmt(v, ' ' + v_u)}, "
        f"observed {obs}."
    )


# =========================
# Tool (no parameters)
# =========================

class EmptyInput(BaseModel):
    pass

def _geomet_mtl_weather_text_bundle() -> Dict[str, Any]:
    meta = {
        "tool": "geomet_mtl_weather_text_bundle",
        "version": "1.0",
        "generated_at_utc": _utc_now_iso(),
        "language": "en",
    }

    try:
        city_raw = _requests_get(CITYPAGE_ENDPOINT, params={"f": "json", "lang": "en"})
        props = city_raw.get("properties") or {}

        city_now_text = _city_now_text_from_citypage(props, lang="en")
        forecast_text = _forecast_texts_from_citypage(props, n_periods=4, lang="en")
        official_warnings = _warnings_from_citypage(props, lang="en")

        stations_all = fetch_swob_bbox(MTL_BBOX, limit=500)
        stations_fixed = pick_fixed_stations(stations_all)
        stations_text = [{"key": s.get("key"), "label": s.get("label"), "text": _station_text(s)} for s in stations_fixed]

        return {
            "meta": meta,
            "city_now_text": city_now_text,
            "forecast_text": forecast_text,
            "stations_text": stations_text,
            "official_warnings": official_warnings,
            "sources": {
                "citypage_endpoint": CITYPAGE_ENDPOINT,
                "swob_endpoint": SWOB_ITEMS_ENDPOINT,
            },
        }

    except requests.HTTPError as e:
        return {"meta": meta, "error": {"type": "HTTPError", "message": str(e)}}
    except Exception as e:
        return {"meta": meta, "error": {"type": type(e).__name__, "message": str(e)}}

geomet_mtl_weather_text_bundle = StructuredTool.from_function(
    func=_geomet_mtl_weather_text_bundle,
    name="geomet_mtl_weather_text_bundle",
    description=(
        "Returns an English, text-first JSON bundle for Montreal mobility use: "
        "city_now_text + forecast_text (text summaries) + stations_text (one line per fixed station)."
    ),
    args_schema=EmptyInput,
)

if __name__ == "__main__":
    import json
    print(json.dumps(geomet_mtl_weather_text_bundle.invoke({}), indent=2, ensure_ascii=False))