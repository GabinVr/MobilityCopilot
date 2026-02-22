from __future__ import annotations

import datetime as dt
import calendar
import math
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple, Protocol
from enum import Enum

import requests


# =========================
# Constants
# =========================

GEOMET_BASE = "https://api.weather.gc.ca"

# Endpoints
CITYPAGE_MTL_ITEM_ID = "qc-147"
CITYPAGE_ENDPOINT = f"{GEOMET_BASE}/collections/citypageweather-realtime/items/{CITYPAGE_MTL_ITEM_ID}"
SWOB_ITEMS_ENDPOINT = f"{GEOMET_BASE}/collections/swob-realtime/items"
CLIMATE_DAILY_ITEMS = f"{GEOMET_BASE}/collections/climate-daily/items"
CLIMATE_MONTHLY_ITEMS = f"{GEOMET_BASE}/collections/climate-monthly/items"

# Geographic bounds
MTL_BBOX: Tuple[float, float, float, float] = (-74.35, 45.35, -73.35, 45.80)

# HTTP settings
TIMEOUT_S = 20
USER_AGENT = "MobilityCopilot/1.0 (Montreal weather API)"

# Historical data limits
MAX_HISTORICAL_PERIODS = 50

# Reference station for Montreal
MTL_REFERENCE_STATION = {
    "name": "Montreal (global proxy via reference station)",
    "label": "Montreal/Trudeau International",
    "climate_identifier": "7025251",
}

# Properties to fetch from SWOB
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


# =========================
# Enums
# =========================

class Frequency(Enum):
    """Fréquences d'agrégation pour les données historiques."""
    WEEK = "week"
    MONTH = "month"
    YEAR = "year"
    
    @classmethod
    def from_string(cls, value: str) -> Frequency:
        """Convertit une chaîne en Frequency (supporte FR/EN)."""
        aliases = {
            "week": cls.WEEK,
            "weekly": cls.WEEK,
            "semaine": cls.WEEK,
            "mois": cls.MONTH,
            "month": cls.MONTH,
            "monthly": cls.MONTH,
            "annee": cls.YEAR,
            "année": cls.YEAR,
            "year": cls.YEAR,
            "yearly": cls.YEAR,
        }
        normalized = value.strip().lower()
        if normalized not in aliases:
            raise ValueError(
                f"Fréquence invalide '{value}'. "
                f"Utilisez: week|month|year (ou semaine|mois|année)."
            )
        return aliases[normalized]


# =========================
# Data Models
# =========================

@dataclass
class CurrentConditions:
    """Conditions météo actuelles."""
    temperature: Optional[float] = None
    temperature_unit: str = "C"
    condition: Optional[str] = None
    wind_direction: Optional[str] = None
    wind_speed: Optional[float] = None
    wind_gust: Optional[float] = None
    wind_speed_unit: str = "km/h"
    wind_chill: Optional[float] = None
    relative_humidity: Optional[float] = None
    observed_at: Optional[str] = None
    
    def to_text(self) -> str:
        """Convertit en texte lisible."""
        parts = []
        if self.temperature is not None:
            parts.append(f"{self.temperature:.1f}°{self.temperature_unit}")
        if self.condition:
            parts.append(self.condition)
        if self.wind_direction and self.wind_speed is not None:
            parts.append(f"vent {self.wind_direction} {self.wind_speed:.0f} {self.wind_speed_unit}")
        if self.wind_gust is not None and self.wind_gust > 0:
            parts.append(f"rafales {self.wind_gust:.0f} {self.wind_speed_unit}")
        if self.wind_chill is not None:
            parts.append(f"refroidissement éolien {self.wind_chill:.0f}°C")
        if self.relative_humidity is not None:
            parts.append(f"HR {self.relative_humidity:.0f}%")
        if self.observed_at:
            parts.append(f"observé {self.observed_at}")
        
        return "Montréal maintenant: " + ", ".join(parts) + "." if parts else "Montréal maintenant: N/D."


@dataclass
class ForecastPeriod:
    """Période de prévision."""
    period_name: Optional[str] = None
    text_summary: Optional[str] = None


@dataclass
class WeatherWarning:
    """Avertissement météo officiel."""
    title: Optional[str] = None
    text: Optional[str] = None


@dataclass
class StationData:
    """Données d'une station météo."""
    key: str
    label: str
    msc_id: Optional[str] = None
    icao_id: Optional[str] = None
    name: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    air_temperature: Optional[float] = None
    temperature_unit: str = "°C"
    visibility: Optional[float] = None
    visibility_unit: str = "km"
    precip_1h: Optional[float] = None
    precip_unit: str = "mm"
    observed_at_utc: Optional[str] = None
    status: str = "ok"
    
    def to_text(self) -> str:
        """Convertit en texte lisible."""
        if self.status != "ok":
            return f"{self.label}: indisponible."
        
        temp = self._format(self.air_temperature, self.temperature_unit, 1)
        vis = self._format(self.visibility, self.visibility_unit, 0)
        precip = self._format(self.precip_1h, self.precip_unit, 0)
        obs = self.observed_at_utc or "N/D"
        
        return (
            f"{self.label}: "
            f"temp {temp}, précip_1h {precip}, "
            f"visibilité {vis}, "
            f"observé {obs}."
        )
    
    @staticmethod
    def _format(value: Optional[float], unit: str, digits: int) -> str:
        """Formate une valeur avec unité."""
        if value is None:
            return "N/D"
        if digits <= 0:
            return f"{value:.0f}{unit}"
        return f"{value:.{digits}f}{unit}"


@dataclass
class HistoricalPeriod:
    """Période de données historiques agrégées."""
    period_id: str
    start_date: str
    end_date: str
    n_obs: int
    mean_temp_c: Optional[float] = None
    min_temp_c: Optional[float] = None
    max_temp_c: Optional[float] = None
    total_precip: Optional[float] = None
    total_snow: Optional[float] = None


# =========================
# Protocols / Interfaces
# =========================

class WeatherDataFetcher(Protocol):
    """Interface pour récupérer des données météo."""
    
    def fetch(self) -> Dict[str, Any]:
        """Récupère les données brutes de l'API."""
        ...


class WeatherDataParser(Protocol):
    """Interface pour parser des données météo."""
    
    def parse(self, data: Dict[str, Any]) -> Any:
        """Parse les données brutes en modèles."""
        ...


# =========================
# HTTP Client
# =========================

class HTTPClient:
    """Client HTTP réutilisable avec gestion d'erreurs."""
    
    def __init__(self, timeout: int = TIMEOUT_S, user_agent: str = USER_AGENT):
        self.timeout = timeout
        self.user_agent = user_agent
    
    def get(self, url: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Effectue une requête GET et retourne du JSON."""
        response = requests.get(
            url,
            params=params,
            timeout=self.timeout,
            headers={
                "User-Agent": self.user_agent,
                "Accept": "application/json,application/geo+json"
            },
        )
        response.raise_for_status()
        
        data = response.json()
        if not isinstance(data, dict):
            raise ValueError(f"Réponse JSON invalide de {url}, reçu {type(data)}")
        
        return data
    
    def follow_pagination(
        self,
        initial_url: str,
        initial_params: Optional[Dict[str, Any]] = None,
        max_pages: int = 200
    ) -> List[Dict[str, Any]]:
        """Suit la pagination et récupère toutes les features."""
        all_features: List[Dict[str, Any]] = []
        url = initial_url
        params = initial_params
        page = 0
        
        while page < max_pages:
            data = self.get(url, params)
            features = data.get("features", []) or []
            all_features.extend(features)
            
            # Cherche le lien "next"
            next_url = self._find_next_link(data)
            if not next_url:
                break
            
            url = next_url
            params = None  # Les params sont dans l'URL next
            page += 1
        
        return all_features
    
    @staticmethod
    def _find_next_link(data: Dict[str, Any]) -> Optional[str]:
        """Trouve le lien de pagination suivant."""
        for link in data.get("links", []) or []:
            if isinstance(link, dict) and link.get("rel") == "next":
                return link.get("href")
        return None


# =========================
# Utility Functions
# =========================

class WeatherUtils:
    """Fonctions utilitaires pour la météo."""
    
    @staticmethod
    def utc_now_iso() -> str:
        """Retourne l'heure UTC actuelle en ISO."""
        return dt.datetime.now(dt.timezone.utc).isoformat()
    
    @staticmethod
    def parse_date(date_str: str) -> dt.date:
        """Parse une date ISO."""
        return dt.date.fromisoformat(date_str)
    
    @staticmethod
    def safe_float(value: Any) -> Optional[float]:
        """Convertit en float de manière sûre."""
        if value is None or isinstance(value, bool):
            return None
        try:
            return float(value)
        except Exception:
            return None
    
    @staticmethod
    def pick_lang(value: Any, lang: str = "en") -> Any:
        """Sélectionne la langue dans un dict multilingue."""
        if isinstance(value, dict):
            if lang in value:
                return value[lang]
            if "en" in value:
                return value["en"]
            if "fr" in value:
                return value["fr"]
        return value
    
    @staticmethod
    def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """Calcule la distance haversine en km."""
        r = 6371.0
        p1, p2 = math.radians(lat1), math.radians(lat2)
        dphi = math.radians(lat2 - lat1)
        dl = math.radians(lon2 - lon1)
        a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
        return 2 * r * math.asin(math.sqrt(a))
    
    @staticmethod
    def round_floats(obj: Any, ndigits: int = 2) -> Any:
        """Arrondit récursivement les floats dans un objet."""
        if isinstance(obj, bool):
            return obj
        if isinstance(obj, float):
            rounded = round(obj, ndigits)
            return 0.0 if rounded == 0 else rounded
        if isinstance(obj, dict):
            return {k: WeatherUtils.round_floats(v, ndigits) for k, v in obj.items()}
        if isinstance(obj, list):
            return [WeatherUtils.round_floats(v, ndigits) for v in obj]
        return obj


# =========================
# Base Weather Client
# =========================

class BaseWeatherClient(ABC):
    """Classe abstraite de base pour les clients météo."""
    
    def __init__(self, http_client: Optional[HTTPClient] = None):
        self.http_client = http_client or HTTPClient()
        self.utils = WeatherUtils()
    
    @abstractmethod
    def fetch_data(self, **kwargs) -> Dict[str, Any]:
        """Récupère et retourne les données météo formatées."""
        pass


# =========================
# Current Weather Client
# =========================

class CurrentWeatherClient(BaseWeatherClient):
    """Client pour les données météo en temps réel (CityPage + SWOB)."""
    
    def __init__(self, http_client: Optional[HTTPClient] = None, lang: str = "fr"):
        super().__init__(http_client)
        self.lang = lang
    
    def fetch_data(self, include_stations: bool = True) -> Dict[str, Any]:
        """
        Récupère les données météo actuelles pour Montréal.
        
        Args:
            include_stations: Inclure les données des stations SWOB
            
        Returns:
            Dict contenant current_conditions, forecasts, warnings, stations
        """
        meta = {
            "tool": "current_weather",
            "version": "1.0",
            "generated_at_utc": self.utils.utc_now_iso(),
            "language": self.lang,
        }
        
        try:
            # Données CityPage
            citypage_data = self.http_client.get(
                CITYPAGE_ENDPOINT,
                params={"f": "json", "lang": self.lang}
            )
            
            props = citypage_data.get("properties") or {}
            
            # Parse les conditions actuelles
            current = self._parse_current_conditions(props)
            
            # Parse les prévisions
            forecasts = self._parse_forecasts(props, n_periods=4)
            
            # Parse les avertissements
            warnings = self._parse_warnings(props)
            
            result = {
                "meta": meta,
                "current_conditions": self._current_to_dict(current),
                "forecasts": [self._forecast_to_dict(f) for f in forecasts],
                "warnings": [self._warning_to_dict(w) for w in warnings],
            }
            
            # Ajoute les stations si demandé
            if include_stations:
                stations = self._fetch_stations()
                result["stations"] = [self._station_to_dict(s) for s in stations]
            
            return result
            
        except requests.HTTPError as e:
            return {"meta": meta, "error": {"type": "HTTPError", "message": str(e)}}
        except Exception as e:
            return {"meta": meta, "error": {"type": type(e).__name__, "message": str(e)}}
    
    def _parse_current_conditions(self, props: Dict[str, Any]) -> CurrentConditions:
        """Parse les conditions actuelles depuis CityPage."""
        cc = props.get("currentConditions") or {}
        
        temp_data = cc.get("temperature") or {}
        temp = self.utils.safe_float(self.utils.pick_lang(temp_data.get("value"), self.lang))
        temp_unit = self.utils.pick_lang(temp_data.get("units"), self.lang) or "C"
        
        condition = self.utils.pick_lang(cc.get("condition"), self.lang)
        
        wc = self.utils.safe_float(
            self.utils.pick_lang((cc.get("windChill") or {}).get("value"), self.lang)
        )
        rh = self.utils.safe_float(
            self.utils.pick_lang((cc.get("relativeHumidity") or {}).get("value"), self.lang)
        )
        
        wind = cc.get("wind") or {}
        w_dir = self.utils.pick_lang((wind.get("direction") or {}).get("value"), self.lang)
        w_spd = self.utils.safe_float(
            self.utils.pick_lang((wind.get("speed") or {}).get("value"), self.lang)
        )
        w_gst = self.utils.safe_float(
            self.utils.pick_lang((wind.get("gust") or {}).get("value"), self.lang)
        )
        w_unit = self.utils.pick_lang((wind.get("speed") or {}).get("units"), self.lang) or "km/h"
        
        observed = self.utils.pick_lang(cc.get("timestamp"), self.lang)
        
        return CurrentConditions(
            temperature=temp,
            temperature_unit=temp_unit,
            condition=condition,
            wind_direction=w_dir,
            wind_speed=w_spd,
            wind_gust=w_gst,
            wind_speed_unit=w_unit,
            wind_chill=wc,
            relative_humidity=rh,
            observed_at=observed,
        )
    
    def _parse_forecasts(
        self,
        props: Dict[str, Any],
        n_periods: int = 4
    ) -> List[ForecastPeriod]:
        """Parse les prévisions depuis CityPage."""
        fg = props.get("forecastGroup") or {}
        forecasts = fg.get("forecasts") or []
        
        if not isinstance(forecasts, list):
            return []
        
        result = []
        for f in forecasts[:n_periods]:
            if not isinstance(f, dict):
                continue
            
            period = f.get("period") or {}
            period_name = self.utils.pick_lang(period.get("textForecastName"), self.lang)
            text_summary = self.utils.pick_lang(f.get("textSummary"), self.lang)
            
            if period_name or text_summary:
                result.append(ForecastPeriod(
                    period_name=period_name,
                    text_summary=text_summary
                ))
        
        return result
    
    def _parse_warnings(self, props: Dict[str, Any]) -> List[WeatherWarning]:
        """Parse les avertissements depuis CityPage."""
        warnings = props.get("warnings") or []
        if not isinstance(warnings, list):
            warnings = [warnings] if warnings else []
        
        result = []
        for w in warnings:
            if not isinstance(w, dict):
                continue
            
            title = self.utils.pick_lang(w.get("title"), self.lang)
            text = (
                self.utils.pick_lang(w.get("text"), self.lang) or
                self.utils.pick_lang(w.get("textSummary"), self.lang)
            )
            
            if title or text:
                result.append(WeatherWarning(title=title, text=text))
        
        return result
    
    def _fetch_stations(self) -> List[StationData]:
        """Récupère et sélectionne les stations fixes de Montréal."""
        # Récupère toutes les stations dans la bbox
        params = {
            "f": "json",
            "lang": "en",
            "bbox": f"{MTL_BBOX[0]},{MTL_BBOX[1]},{MTL_BBOX[2]},{MTL_BBOX[3]}",
            "limit": 500,
            "sortby": "-date_tm-value",
            "properties": ",".join(SWOB_PROPERTIES),
        }
        
        raw_data = self.http_client.get(SWOB_ITEMS_ENDPOINT, params=params)
        features = raw_data.get("features") or []
        
        # Parse toutes les stations
        all_stations = [self._parse_station(f) for f in features if isinstance(f, dict)]
        all_stations = [s for s in all_stations if s is not None]
        
        # Sélectionne les stations fixes
        return self._select_fixed_stations(all_stations)
    
    def _parse_station(self, feature: Dict[str, Any]) -> Optional[StationData]:
        """Parse une feature SWOB en StationData."""
        props = feature.get("properties") or {}
        geom = feature.get("geometry") or {}
        coords = geom.get("coordinates")
        
        # Précipitation: préfère total, sinon pluie
        precip = props.get("pcpn_amt_pst1hr")
        precip_u = props.get("pcpn_amt_pst1hr-uom") or "mm"
        if precip is None:
            precip = props.get("rnfl_amt_pst1hr")
            precip_u = props.get("rnfl_amt_pst1hr-uom") or precip_u
        
        lat = coords[1] if isinstance(coords, list) and len(coords) >= 2 else None
        lon = coords[0] if isinstance(coords, list) and len(coords) >= 2 else None
        
        return StationData(
            key="",  # Sera défini lors de la sélection
            label="",  # Sera défini lors de la sélection
            msc_id=props.get("msc_id-value"),
            icao_id=props.get("icao_stn_id-value"),
            name=props.get("stn_nam-value"),
            latitude=self.utils.safe_float(lat),
            longitude=self.utils.safe_float(lon),
            air_temperature=self.utils.safe_float(props.get("air_temp")),
            temperature_unit=props.get("air_temp-uom") or "°C",
            visibility=self.utils.safe_float(props.get("vis")),
            visibility_unit=props.get("vis-uom") or "km",
            precip_1h=self.utils.safe_float(precip),
            precip_unit=precip_u,
            observed_at_utc=props.get("date_tm-value"),
        )
    
    def _select_fixed_stations(self, all_stations: List[StationData]) -> List[StationData]:
        """Sélectionne les stations fixes de Montréal."""
        # Profils des stations fixes
        profiles = [
            {
                "key": "downtown",
                "label": "Centre-ville / McGill (McTavish)",
                "preferred_msc_ids": ["7024745"],
                "preferred_icao_ids": [],
                "name_contains": ["MCTAVISH"],
                "anchor": (45.504926, -73.579185),
            },
            {
                "key": "airport_trudeau",
                "label": "Aéroport (Trudeau)",
                "preferred_msc_ids": ["7025251", "702S006"],
                "preferred_icao_ids": ["CYUL"],
                "name_contains": ["TRUDEAU", "PIERRE ELLIOTT"],
                "anchor": (45.4705, -73.7409),
            },
            {
                "key": "south_shore",
                "label": "Rive-Sud (St-Hubert)",
                "preferred_msc_ids": ["7027329"],
                "preferred_icao_ids": ["CYHU"],
                "name_contains": ["ST-HUBERT", "HUBERT"],
                "anchor": (45.5181, -73.4169),
            },
            {
                "key": "north",
                "label": "Nord (Mirabel)",
                "preferred_msc_ids": ["7034900"],
                "preferred_icao_ids": ["CYMX"],
                "name_contains": ["MIRABEL"],
                "anchor": (45.6804, -74.0387),
            },
            {
                "key": "west_island",
                "label": "Ouest-de-l'Île (Ste-Anne-de-Bellevue)",
                "preferred_msc_ids": ["702FHL8"],
                "preferred_icao_ids": [],
                "name_contains": ["STE-ANNE", "BELLEVUE"],
                "anchor": (45.4270, -73.92892),
            },
        ]
        
        # Index par MSC ID et ICAO ID
        by_msc = {s.msc_id: s for s in all_stations if s.msc_id}
        by_icao = {s.icao_id.upper(): s for s in all_stations if s.icao_id}
        
        selected = []
        used_ids = set()
        
        for prof in profiles:
            chosen = None
            
            # Cherche par MSC ID préféré
            for msc in prof.get("preferred_msc_ids", []):
                if msc in by_msc:
                    chosen = by_msc[msc]
                    break
            
            # Cherche par ICAO ID préféré
            if chosen is None:
                for icao in prof.get("preferred_icao_ids", []):
                    if icao.upper() in by_icao:
                        chosen = by_icao[icao.upper()]
                        break
            
            # Cherche par nom
            if chosen is None and prof.get("name_contains"):
                chosen = self._find_by_name(all_stations, prof["name_contains"])
            
            # Cherche par proximité
            if chosen is None and prof.get("anchor"):
                chosen = self._find_nearest(all_stations, prof["anchor"])
            
            # Si pas trouvé, marque comme indisponible
            if chosen is None:
                selected.append(StationData(
                    key=prof["key"],
                    label=prof["label"],
                    status="unavailable"
                ))
                continue
            
            # Clone et configure
            station = StationData(
                key=prof["key"],
                label=prof["label"],
                msc_id=chosen.msc_id,
                icao_id=chosen.icao_id,
                name=chosen.name,
                latitude=chosen.latitude,
                longitude=chosen.longitude,
                air_temperature=chosen.air_temperature,
                temperature_unit=chosen.temperature_unit,
                visibility=chosen.visibility,
                visibility_unit=chosen.visibility_unit,
                precip_1h=chosen.precip_1h,
                precip_unit=chosen.precip_unit,
                observed_at_utc=chosen.observed_at_utc,
                status="ok"
            )
            
            # Évite les doublons
            dedup_key = station.msc_id or station.icao_id or station.name
            if dedup_key not in used_ids:
                used_ids.add(dedup_key)
                selected.append(station)
        
        return selected
    
    @staticmethod
    def _find_by_name(stations: List[StationData], needles: List[str]) -> Optional[StationData]:
        """Trouve une station par correspondance de nom."""
        needles_upper = [n.upper() for n in needles if n]
        for s in stations:
            name_upper = (s.name or "").strip().upper()
            if any(n in name_upper for n in needles_upper):
                return s
        return None
    
    def _find_nearest(
        self,
        stations: List[StationData],
        anchor: Tuple[float, float]
    ) -> Optional[StationData]:
        """Trouve la station la plus proche d'un point."""
        a_lat, a_lon = anchor
        best = None
        best_dist = float('inf')
        
        for s in stations:
            if s.latitude is not None and s.longitude is not None:
                dist = self.utils.haversine_km(a_lat, a_lon, s.latitude, s.longitude)
                if dist < best_dist:
                    best_dist = dist
                    best = s
        
        return best
    
    @staticmethod
    def _current_to_dict(cond: CurrentConditions) -> Dict[str, Any]:
        """Convertit CurrentConditions en dict."""
        return {
            "temperature": cond.temperature,
            "temperature_unit": cond.temperature_unit,
            "condition": cond.condition,
            "wind_direction": cond.wind_direction,
            "wind_speed": cond.wind_speed,
            "wind_gust": cond.wind_gust,
            "wind_speed_unit": cond.wind_speed_unit,
            "wind_chill": cond.wind_chill,
            "relative_humidity": cond.relative_humidity,
            "observed_at": cond.observed_at,
        }
    
    @staticmethod
    def _forecast_to_dict(fc: ForecastPeriod) -> Dict[str, Any]:
        """Convertit ForecastPeriod en dict."""
        return {
            "period_name": fc.period_name,
            "text_summary": fc.text_summary,
        }
    
    @staticmethod
    def _warning_to_dict(w: WeatherWarning) -> Dict[str, Any]:
        """Convertit WeatherWarning en dict."""
        return {
            "title": w.title,
            "text": w.text,
        }
    
    @staticmethod
    def _station_to_dict(s: StationData) -> Dict[str, Any]:
        """Convertit StationData en dict."""
        return {
            "key": s.key,
            "label": s.label,
            "msc_id": s.msc_id,
            "icao_id": s.icao_id,
            "name": s.name,
            "latitude": s.latitude,
            "longitude": s.longitude,
            "air_temperature": s.air_temperature,
            "temperature_unit": s.temperature_unit,
            "visibility": s.visibility,
            "visibility_unit": s.visibility_unit,
            "precip_1h": s.precip_1h,
            "precip_unit": s.precip_unit,
            "observed_at_utc": s.observed_at_utc,
            "status": s.status,
        }


# =========================
# Historical Weather Client
# =========================

class HistoricalWeatherClient(BaseWeatherClient):
    """Client pour les données météo historiques."""
    
    def __init__(self, http_client: Optional[HTTPClient] = None):
        super().__init__(http_client)
        self.climate_id = MTL_REFERENCE_STATION["climate_identifier"]
    
    def fetch_data(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        frequency: str = "month"
    ) -> Dict[str, Any]:
        """
        Récupère les données historiques agrégées.
        
        Args:
            start_date: Date de début (YYYY-MM-DD), None = auto (max 50 périodes)
            end_date: Date de fin (YYYY-MM-DD), None = aujourd'hui
            frequency: week | month | year (ou semaine | mois | année)
            
        Returns:
            Dict contenant les périodes agrégées
        """
        meta = {
            "tool": "historical_weather",
            "version": "1.0",
            "generated_at_utc": self.utils.utc_now_iso(),
            "max_periods": MAX_HISTORICAL_PERIODS,
        }
        
        try:
            freq = Frequency.from_string(frequency)
            
            # Dates
            end = self.utils.parse_date(end_date) if end_date else dt.datetime.now(dt.timezone.utc).date()
            req_start = self.utils.parse_date(start_date) if start_date else None
            
            if req_start and req_start > end:
                raise ValueError("start_date doit être <= end_date")
            
            # Calcule le start effectif (max 50 périodes)
            eff_start, truncated = self._infer_effective_start(req_start, end, freq)
            
            # Récupère et agrège les données
            if freq == Frequency.WEEK:
                periods = self._fetch_weekly(eff_start, end)
            elif freq == Frequency.MONTH:
                periods = self._fetch_monthly(eff_start, end)
            else:  # YEAR
                periods = self._fetch_yearly(eff_start, end)
            
            # Limite à MAX_HISTORICAL_PERIODS
            if len(periods) > MAX_HISTORICAL_PERIODS:
                periods = periods[-MAX_HISTORICAL_PERIODS:]
                truncated = True
            
            summary = (
                f"Montréal historique ({freq.value}) de {eff_start.isoformat()} "
                f"à {end.isoformat()} ({len(periods)} périodes, max {MAX_HISTORICAL_PERIODS})."
            )
            
            return {
                "meta": meta,
                "summary": summary,
                "query": {
                    "requested_start_date": req_start.isoformat() if req_start else None,
                    "requested_end_date": end.isoformat(),
                    "effective_start_date": eff_start.isoformat(),
                    "effective_end_date": end.isoformat(),
                    "frequency": freq.value,
                    "truncated_to_max_periods": truncated,
                },
                "periods": [self._period_to_dict(p) for p in periods],
                "units": {
                    "temperature": "°C",
                    "total_precip": "mm",
                    "total_snow": "cm",
                },
            }
            
        except requests.HTTPError as e:
            return {"meta": meta, "error": {"type": "HTTPError", "message": str(e)}}
        except Exception as e:
            return {"meta": meta, "error": {"type": type(e).__name__, "message": str(e)}}
    
    def _infer_effective_start(
        self,
        requested_start: Optional[dt.date],
        end: dt.date,
        freq: Frequency
    ) -> Tuple[dt.date, bool]:
        """Calcule le start effectif pour limiter à MAX_HISTORICAL_PERIODS."""
        anchor = self._floor_period_start(end, freq)
        earliest = self._shift_period_start(anchor, freq, -(MAX_HISTORICAL_PERIODS - 1))
        
        if requested_start is None:
            return earliest, True
        if requested_start >= earliest:
            return requested_start, False
        return earliest, True
    
    def _fetch_weekly(self, start: dt.date, end: dt.date) -> List[HistoricalPeriod]:
        """Récupère et agrège hebdomadairement depuis climate-daily."""
        features = self._fetch_climate_features(
            CLIMATE_DAILY_ITEMS,
            start.year,
            end.year
        )
        
        # Parse les données journalières
        dailies = []
        for feat in features:
            parsed = self._parse_daily_feature(feat)
            if parsed:
                dailies.append(parsed)
        
        return self._aggregate_to_weeks(dailies, start, end)
    
    def _fetch_monthly(self, start: dt.date, end: dt.date) -> List[HistoricalPeriod]:
        """Récupère les données mensuelles depuis climate-monthly."""
        features = self._fetch_climate_features(
            CLIMATE_MONTHLY_ITEMS,
            start.year,
            end.year
        )
        
        # Parse les données mensuelles
        monthlies = []
        for feat in features:
            parsed = self._parse_monthly_feature(feat)
            if parsed:
                monthlies.append(parsed)
        
        return self._aggregate_to_months(monthlies, start, end)
    
    def _fetch_yearly(self, start: dt.date, end: dt.date) -> List[HistoricalPeriod]:
        """Récupère et agrège annuellement depuis climate-monthly."""
        features = self._fetch_climate_features(
            CLIMATE_MONTHLY_ITEMS,
            start.year,
            end.year
        )
        
        # Parse les données mensuelles
        monthlies = []
        for feat in features:
            parsed = self._parse_monthly_feature(feat)
            if parsed:
                monthlies.append(parsed)
        
        return self._aggregate_to_years(monthlies, start, end)
    
    def _fetch_climate_features(
        self,
        endpoint: str,
        year_start: int,
        year_end: int
    ) -> List[Dict[str, Any]]:
        """Récupère les features climate avec pagination."""
        cql = (
            f"properties.CLIMATE_IDENTIFIER = '{self.climate_id}' "
            f"AND properties.LOCAL_YEAR >= {year_start} "
            f"AND properties.LOCAL_YEAR <= {year_end}"
        )
        
        params = {
            "f": "json",
            "lang": "en",
            "limit": 1000,
            "filter": cql
        }
        
        return self.http_client.follow_pagination(endpoint, params, max_pages=200)
    
    def _parse_daily_feature(self, feat: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Parse une feature climate-daily."""
        props = feat.get("properties") or {}
        
        # Parse la date
        local_date = props.get("LOCAL_DATE")
        d = None
        if isinstance(local_date, str):
            try:
                d = dt.date.fromisoformat(local_date[:10])
            except Exception:
                pass
        
        if d is None:
            try:
                d = dt.date(
                    int(props["LOCAL_YEAR"]),
                    int(props["LOCAL_MONTH"]),
                    int(props["LOCAL_DAY"])
                )
            except Exception:
                return None
        
        return {
            "date": d,
            "mean_temp_c": self.utils.safe_float(props.get("MEAN_TEMPERATURE")),
            "min_temp_c": self.utils.safe_float(props.get("MIN_TEMPERATURE")),
            "max_temp_c": self.utils.safe_float(props.get("MAX_TEMPERATURE")),
            "total_precip": self.utils.safe_float(props.get("TOTAL_PRECIPITATION")),
            "total_snow": self.utils.safe_float(props.get("TOTAL_SNOW")),
        }
    
    def _parse_monthly_feature(self, feat: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Parse une feature climate-monthly."""
        props = feat.get("properties") or {}
        
        try:
            year = int(props["LOCAL_YEAR"])
            month = int(props["LOCAL_MONTH"])
        except Exception:
            return None
        
        return {
            "year": year,
            "month": month,
            "mean_temp_c": self.utils.safe_float(props.get("MEAN_TEMPERATURE")),
            "min_temp_c": self.utils.safe_float(props.get("MIN_TEMPERATURE")),
            "max_temp_c": self.utils.safe_float(props.get("MAX_TEMPERATURE")),
            "total_precip": self.utils.safe_float(props.get("TOTAL_PRECIPITATION")),
            "total_snow": self.utils.safe_float(props.get("TOTAL_SNOWFALL")),
        }
    
    def _aggregate_to_weeks(
        self,
        dailies: List[Dict[str, Any]],
        start: dt.date,
        end: dt.date
    ) -> List[HistoricalPeriod]:
        """Agrège les données journalières en semaines."""
        buckets: Dict[str, Dict[str, Any]] = {}
        
        for rec in dailies:
            d = rec["date"]
            if d < start or d > end:
                continue
            
            # Début de semaine (lundi)
            week_start = d - dt.timedelta(days=d.weekday())
            week_end = week_start + dt.timedelta(days=6)
            iso_year, iso_week, _ = week_start.isocalendar()
            period_id = f"{iso_year}-W{iso_week:02d}"
            
            if period_id not in buckets:
                buckets[period_id] = {
                    "start": week_start,
                    "end": week_end,
                    "n_obs": 0,
                    "temp_sum": 0.0,
                    "temp_n": 0,
                    "min_min": None,
                    "max_max": None,
                    "precip_sum": 0.0,
                    "precip_n": 0,
                    "snow_sum": 0.0,
                    "snow_n": 0,
                }
            
            b = buckets[period_id]
            b["n_obs"] += 1
            
            if rec.get("mean_temp_c") is not None:
                b["temp_sum"] += rec["mean_temp_c"]
                b["temp_n"] += 1
            
            if rec.get("min_temp_c") is not None:
                b["min_min"] = (
                    rec["min_temp_c"] if b["min_min"] is None
                    else min(b["min_min"], rec["min_temp_c"])
                )
            
            if rec.get("max_temp_c") is not None:
                b["max_max"] = (
                    rec["max_temp_c"] if b["max_max"] is None
                    else max(b["max_max"], rec["max_temp_c"])
                )
            
            if rec.get("total_precip") is not None:
                b["precip_sum"] += rec["total_precip"]
                b["precip_n"] += 1
            
            if rec.get("total_snow") is not None:
                b["snow_sum"] += rec["total_snow"]
                b["snow_n"] += 1
        
        # Convertit en HistoricalPeriod
        periods = []
        for pid, b in sorted(buckets.items(), key=lambda x: x[1]["start"]):
            periods.append(HistoricalPeriod(
                period_id=pid,
                start_date=max(b["start"], start).isoformat(),
                end_date=min(b["end"], end).isoformat(),
                n_obs=b["n_obs"],
                mean_temp_c=b["temp_sum"] / b["temp_n"] if b["temp_n"] else None,
                min_temp_c=b["min_min"],
                max_temp_c=b["max_max"],
                total_precip=b["precip_sum"] if b["precip_n"] else None,
                total_snow=b["snow_sum"] if b["snow_n"] else None,
            ))
        
        return periods
    
    def _aggregate_to_months(
        self,
        monthlies: List[Dict[str, Any]],
        start: dt.date,
        end: dt.date
    ) -> List[HistoricalPeriod]:
        """Convertit les données mensuelles en HistoricalPeriod."""
        periods = []
        
        for rec in sorted(monthlies, key=lambda x: (x["year"], x["month"])):
            month_start = dt.date(rec["year"], rec["month"], 1)
            month_end = dt.date(
                rec["year"],
                rec["month"],
                calendar.monthrange(rec["year"], rec["month"])[1]
            )
            
            if month_end < start or month_start > end:
                continue
            
            period_id = f"{rec['year']:04d}-{rec['month']:02d}"
            
            periods.append(HistoricalPeriod(
                period_id=period_id,
                start_date=max(month_start, start).isoformat(),
                end_date=min(month_end, end).isoformat(),
                n_obs=1,
                mean_temp_c=rec.get("mean_temp_c"),
                min_temp_c=rec.get("min_temp_c"),
                max_temp_c=rec.get("max_temp_c"),
                total_precip=rec.get("total_precip"),
                total_snow=rec.get("total_snow"),
            ))
        
        return periods
    
    def _aggregate_to_years(
        self,
        monthlies: List[Dict[str, Any]],
        start: dt.date,
        end: dt.date
    ) -> List[HistoricalPeriod]:
        """Agrège les données mensuelles en années."""
        buckets: Dict[int, Dict[str, Any]] = {}
        
        for rec in monthlies:
            month_start = dt.date(rec["year"], rec["month"], 1)
            month_end = dt.date(
                rec["year"],
                rec["month"],
                calendar.monthrange(rec["year"], rec["month"])[1]
            )
            
            if month_end < start or month_start > end:
                continue
            
            year = rec["year"]
            if year not in buckets:
                buckets[year] = {
                    "start": dt.date(year, 1, 1),
                    "end": dt.date(year, 12, 31),
                    "n_obs": 0,
                    "temp_sum": 0.0,
                    "temp_n": 0,
                    "min_min": None,
                    "max_max": None,
                    "precip_sum": 0.0,
                    "precip_n": 0,
                    "snow_sum": 0.0,
                    "snow_n": 0,
                }
            
            b = buckets[year]
            b["n_obs"] += 1
            
            # Pondère la température moyenne par le nombre de jours
            if rec.get("mean_temp_c") is not None:
                days = calendar.monthrange(rec["year"], rec["month"])[1]
                b["temp_sum"] += rec["mean_temp_c"] * days
                b["temp_n"] += days
            
            if rec.get("min_temp_c") is not None:
                b["min_min"] = (
                    rec["min_temp_c"] if b["min_min"] is None
                    else min(b["min_min"], rec["min_temp_c"])
                )
            
            if rec.get("max_temp_c") is not None:
                b["max_max"] = (
                    rec["max_temp_c"] if b["max_max"] is None
                    else max(b["max_max"], rec["max_temp_c"])
                )
            
            if rec.get("total_precip") is not None:
                b["precip_sum"] += rec["total_precip"]
                b["precip_n"] += 1
            
            if rec.get("total_snow") is not None:
                b["snow_sum"] += rec["total_snow"]
                b["snow_n"] += 1
        
        # Convertit en HistoricalPeriod
        periods = []
        for year, b in sorted(buckets.items()):
            periods.append(HistoricalPeriod(
                period_id=f"{year:04d}",
                start_date=max(b["start"], start).isoformat(),
                end_date=min(b["end"], end).isoformat(),
                n_obs=b["n_obs"],
                mean_temp_c=b["temp_sum"] / b["temp_n"] if b["temp_n"] else None,
                min_temp_c=b["min_min"],
                max_temp_c=b["max_max"],
                total_precip=b["precip_sum"] if b["precip_n"] else None,
                total_snow=b["snow_sum"] if b["snow_n"] else None,
            ))
        
        return periods
    
    @staticmethod
    def _floor_period_start(d: dt.date, freq: Frequency) -> dt.date:
        """Arrondit une date au début de période."""
        if freq == Frequency.WEEK:
            return d - dt.timedelta(days=d.weekday())
        if freq == Frequency.MONTH:
            return dt.date(d.year, d.month, 1)
        if freq == Frequency.YEAR:
            return dt.date(d.year, 1, 1)
        raise ValueError(f"Fréquence non supportée: {freq}")
    
    @staticmethod
    def _shift_period_start(d: dt.date, freq: Frequency, n: int) -> dt.date:
        """Décale une date de n périodes."""
        if n == 0:
            return d
        
        if freq == Frequency.WEEK:
            return d + dt.timedelta(days=7 * n)
        
        if freq == Frequency.MONTH:
            year, month = d.year, d.month + n
            while month <= 0:
                month += 12
                year -= 1
            while month > 12:
                month -= 12
                year += 1
            return dt.date(year, month, 1)
        
        if freq == Frequency.YEAR:
            return dt.date(d.year + n, 1, 1)
        
        raise ValueError(f"Fréquence non supportée: {freq}")
    
    @staticmethod
    def _period_to_dict(period: HistoricalPeriod) -> Dict[str, Any]:
        """Convertit un HistoricalPeriod en dict avec arrondis."""
        return WeatherUtils.round_floats({
            "period_id": period.period_id,
            "start_date": period.start_date,
            "end_date": period.end_date,
            "n_obs": period.n_obs,
            "mean_temp_c": period.mean_temp_c,
            "min_temp_c": period.min_temp_c,
            "max_temp_c": period.max_temp_c,
            "total_precip": period.total_precip,
            "total_snow": period.total_snow,
        }, ndigits=2)


# =========================
# Weather API Facade
# =========================

class MontrealWeatherAPI:
    """
    Façade principale pour l'API météo de Montréal.
    
    Fournit un point d'accès unifié aux données actuelles et historiques.
    """
    
    def __init__(
        self,
        http_client: Optional[HTTPClient] = None,
        lang: str = "fr"
    ):
        """
        Initialise l'API météo.
        
        Args:
            http_client: Client HTTP personnalisé (optionnel)
            lang: Langue pour les données actuelles ('fr' ou 'en')
        """
        self.http_client = http_client or HTTPClient()
        self.current_client = CurrentWeatherClient(self.http_client, lang=lang)
        self.historical_client = HistoricalWeatherClient(self.http_client)
    
    def get_current_weather(self, include_stations: bool = True) -> Dict[str, Any]:
        """
        Récupère les conditions météo actuelles.
        
        Args:
            include_stations: Inclure les stations SWOB
            
        Returns:
            Dict avec current_conditions, forecasts, warnings, stations
        """
        return self.current_client.fetch_data(include_stations=include_stations)
    
    def get_historical_weather(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        frequency: str = "month"
    ) -> Dict[str, Any]:
        """
        Récupère les données historiques agrégées.
        
        Args:
            start_date: Date de début 'YYYY-MM-DD' (None = auto, max 50 périodes)
            end_date: Date de fin 'YYYY-MM-DD' (None = aujourd'hui)
            frequency: 'week'|'month'|'year' (ou 'semaine'|'mois'|'année')
            
        Returns:
            Dict avec périodes agrégées
        """
        return self.historical_client.fetch_data(
            start_date=start_date,
            end_date=end_date,
            frequency=frequency
        )


# =========================
# CLI / Testing
# =========================

if __name__ == "__main__":
    import json
    
    print("="*70)
    print("Test de l'API Météo de Montréal")
    print("="*70)
    
    api = MontrealWeatherAPI(lang="fr")
    
    # Test météo actuelle
    print("\n### MÉTÉO ACTUELLE ###\n")
    current = api.get_current_weather(include_stations=True)
    
    if "error" in current:
        print(f"Erreur: {current['error']}")
    else:
        # Affiche les conditions
        if "current_conditions" in current:
            cond_dict = current["current_conditions"]
            cond = CurrentConditions(**cond_dict)
            print(cond.to_text())
        
        # Affiche les prévisions
        if "forecasts" in current and current["forecasts"]:
            print("\nPrévisions:")
            for fc_dict in current["forecasts"][:2]:
                fp = ForecastPeriod(**fc_dict)
                print(f"  {fp.period_name}: {fp.text_summary}")
        
        # Affiche quelques stations
        if "stations" in current and current["stations"]:
            print("\nStations:")
            for st_dict in current["stations"][:3]:
                station = StationData(**st_dict)
                print(f"  {station.to_text()}")
    
    # Test données historiques
    print("\n### DONNÉES HISTORIQUES (dernières 12 mois) ###\n")
    historical = api.get_historical_weather(
        start_date="2025-02-01",
        end_date="2026-02-22",
        frequency="month"
    )
    
    if "error" in historical:
        print(f"Erreur: {historical['error']}")
    else:
        print(historical.get("summary", ""))
        if "periods" in historical and historical["periods"]:
            print(f"\nNombre de périodes: {len(historical['periods'])}")
            print("\nExemples:")
            for p in historical["periods"][-3:]:
                print(f"  {p['period_id']}: "
                      f"temp moy {p['mean_temp_c']:.1f}°C, "
                      f"précip {p['total_precip']:.1f}mm")
    
    print("\n" + "="*70)
    print("Tests terminés")
    print("="*70)
