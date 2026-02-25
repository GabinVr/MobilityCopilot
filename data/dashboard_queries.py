import os
import pandas as pd
import numpy as np
import json
from abc import ABC, abstractmethod
from typing import Dict, Any, List
import sqlite3
import re
import os
from data.weather_api import MontrealWeatherAPI
from pathlib import Path
import logging
from cache import redis_cache


LOCAL_DIR = Path(__file__).parent
DEFAULT_DB_PATH = os.path.join(LOCAL_DIR, "db/mobility.db")
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

def parse_time_range(time_range: str) -> tuple[str, str]:
    """ 
    Parse a time range string like 'last_month', 'last_week' or 'YYYY-MM-DD to YYYY-MM-DD' and return the corresponding start and end dates as strings in 'YYYY-MM-DD' format.
    Args:
        time_range (str): The time range to parse.
    Returns:
        tuple[str, str]: A tuple containing the start and end dates as strings.
    Raises:        ValueError: If the time range format is invalid.
    Example usage:
        start_date, end_date = parse_time_range('last_month')
        start_date, end_date = parse_time_range('2023-01-01 to 2023-01-31')
    """
    if time_range == 'last_month':
        end_date = pd.Timestamp.now()
        start_date = end_date - pd.DateOffset(months=1)
    elif time_range == 'last_week':
        end_date = pd.Timestamp.now()
        start_date = end_date - pd.DateOffset(weeks=1)
    else:
        match = re.match(r'(\d{4}-\d{2}-\d{2})\s+to\s+(\d{4}-\d{2}-\d{2})', time_range)
        if not match:
            raise ValueError("Invalid time range format. Use 'last_month', 'last_week', or 'YYYY-MM-DD to YYYY-MM-DD'.")
        start_date, end_date = match.groups()
        return start_date, end_date
    
    return start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')

class DashboardQuery(ABC):
    def __init__(self, db_path: str = DEFAULT_DB_PATH):
        super().__init__()
        self.db_path = db_path
        self.conn = None
    
    def connect(self):  
        if self.conn is None:
            self.conn = sqlite3.connect(self.db_path)
    
    def disconnect(self):
        if self.conn:
            self.conn.close()
            self.conn = None
        
    @abstractmethod
    def execute(self, **kwargs) -> Dict:
        pass

class WordCloudQuery311(DashboardQuery):
    def __init__(self, db_path: str = DEFAULT_DB_PATH):
        super().__init__(db_path)
        self.not_allowed_words = set(["des", "d", "de"])
    
    @redis_cache(expire=3600*3)
    def execute(self, 
                top_n: int,
                time_range: str,
                **kwargs) -> Dict:
        """
        Execute the query to get the top N words from 311 service requests within a specified time range.
        Args:
            top_n (int): The number of top words to return.
            time_range (str): The time range for filtering requests (e.g., 'last_month', 'last_week' or a specific date range like '2023-01-01 to 2023-01-31').
        Returns:
            Dict: A dictionary containing the top N words and their counts.
        Example usage:
            query = WordCloudQuery311()
            result = query.execute(top_n=10, time_range='last_month')
        Return format:
            {
                "top_words": [
                    {"word": "pothole", "count": 150},
                    {"word": "graffiti", "count": 120},
                    ...
                ]
            }
        """
        self.connect()
        cursor = self.conn.cursor()
        logger.debug(f"Executing WordCloudQuery311 with top_n={top_n} and time_range='{time_range}'")
        # Parse time_range to get start and end dates
        start_date, end_date = parse_time_range(time_range)
        # Execute SQL query to get the relevant service requests
        cursor.execute("""
            SELECT ACTI_NOM, DDS_DATE_CREATION FROM requetes311
        """)
        rows = cursor.fetchall()
        self.disconnect()
        # Filter rows based on the time range
        # DDS_DATE_CREATION is in the format 'YYYY-MM-DD HH:MM:SS' or 'YYYY-MM-DDTHH:MM:SS'
        # We will convert it to a datetime object and filter based on the start and end dates
        df_requests = pd.DataFrame(rows, columns=['ACTI_NOM', 'DDS_DATE_CREATION'])
        # Convert ACTI_NOM to utf-8 and handle any decoding issues
        # df_requests['ACTI_NOM'] = df_requests['ACTI_NOM'].apply(lambda x: x.encode('utf-8', errors='ignore').decode('utf-8', errors='ignore') if isinstance(x, str) else x)
        df_requests['DDS_DATE_CREATION'] = pd.to_datetime(df_requests['DDS_DATE_CREATION'])
        logger.debug(f"Total service requests retrieved: {len(df_requests)}")
        df_filtered = df_requests[(df_requests['DDS_DATE_CREATION'] >= start_date) & (df_requests['DDS_DATE_CREATION'] <= end_date)]
        # Count the occurrences of each word in the ACTI_NOM column
        word_counts = {}
        for acti_nom in df_filtered['ACTI_NOM']:
            words = re.findall(r'\w+', acti_nom.lower())
            for word in words:
                word_counts[word] = word_counts.get(word, 0) + 1
        # Get the top N words
        top_words = sorted(word_counts.items(), key=lambda x: x[1] if x[0] not in self.not_allowed_words else 0, reverse=True)[:top_n]
        return {
            "top_words": [{"word": word, "count": count} for word, count in top_words]
        }

class WeatherCorrelationQuery(DashboardQuery):
    def __init__(self, db_path: str = DEFAULT_DB_PATH):
        super().__init__(db_path)
    
    def execute(self, 
                start_date: str,
                end_date: str,
                frequency: str = "week",
                **kwargs) -> Dict:
        """
        Analyse la corrélation entre les conditions météorologiques et les collisions routières.
        
        Args:
            start_date (str): Date de début au format YYYY-MM-DD
            end_date (str): Date de fin au format YYYY-MM-DD
            frequency (str): Fréquence d'agrégation ('week' ou 'month'). Défaut: 'week'
            
        Returns:
            Dict: Dictionnaire contenant l'analyse de corrélation avec :
                - summary: Résumé global (total périodes, total collisions, moyenne)
                - correlations: Liste des périodes avec données météo et collisions
                - temperature_analysis: Statistiques par plages de température
                - precipitation_analysis: Statistiques par plages de précipitations
                - snow_analysis: Statistiques par plages d'enneigement
                - top_periods: Top 5 des périodes avec le plus de collisions
                
        Example usage:
            query = WeatherCorrelationQuery()
            result = query.execute(
                start_date='2021-01-01',
                end_date='2021-03-31',
                frequency='week'
            )
            
        Return format:
            {
                "summary": {
                    "total_periods": 13,
                    "total_collisions": 1250,
                    "avg_collisions_per_period": 96.2
                },
                "correlations": [
                    {
                        "period_id": "2021-W01",
                        "start_date": "2021-01-01",
                        "end_date": "2021-01-07",
                        "weather": {
                            "mean_temp_c": -8.5,
                            "min_temp_c": -15.2,
                            "max_temp_c": -2.1,
                            "total_precip_mm": 12.5,
                            "total_snow_cm": 8.3
                        },
                        "collisions": {
                            "total": 98,
                            "deaths": 0,
                            "severely_injured": 2,
                            "lightly_injured": 15,
                            "by_severity": {"Léger": 45, "Grave": 10, ...}
                        }
                    },
                    ...
                ],
                "temperature_analysis": {
                    "cold": {"periods": 5, "avg_collisions": 102.4},
                    "mild": {"periods": 6, "avg_collisions": 95.1},
                    "warm": {"periods": 2, "avg_collisions": 85.5}
                },
                "precipitation_analysis": {...},
                "snow_analysis": {...},
                "top_periods": [...]
            }
        """
        from collections import defaultdict
                
        # 1. Récupérer les données météo historiques
        weather_api = MontrealWeatherAPI()
        weather_data = weather_api.get_historical_weather(
            start_date=start_date,
            end_date=end_date,
            frequency=frequency
        )
        
        if "error" in weather_data:
            return {"error": f"Weather API error: {weather_data['error']}"}
        
        # 2. Récupérer les données de collisions
        collision_query = CollisionHeatMapQuery(db_path=self.db_path)
        collision_data = collision_query.execute(
            time_range=f"{start_date} to {end_date}"
        )
        
        # 3. Grouper les collisions par période météo
        collisions_by_period = defaultdict(list)
        for collision in collision_data.get("collisions", []):
            date = collision["date"]
            # Convertit YYYY/MM/DD en YYYY-MM-DD pour compatibilité
            normalized_date = date.replace("/", "-")
            
            # Trouve la période correspondante
            for period in weather_data["periods"]:
                if period["start_date"] <= normalized_date <= period["end_date"]:
                    collisions_by_period[period["period_id"]].append(collision)
                    break
        
        # 4. Calculer les statistiques par période
        correlations = []
        
        for period in weather_data["periods"]:
            period_id = period["period_id"]
            collisions = collisions_by_period[period_id]
            
            # Statistiques météo
            mean_temp = period.get("mean_temp_c")
            min_temp = period.get("min_temp_c")
            max_temp = period.get("max_temp_c")
            total_precip = period.get("total_precip", 0) or 0
            total_snow = period.get("total_snow", 0) or 0
            
            # Statistiques collisions
            n_collisions = len(collisions)
            n_deaths = sum(c.get("deaths", 0) for c in collisions)
            n_severe = sum(c.get("severely_injured", 0) for c in collisions)
            n_light = sum(c.get("lightly_injured", 0) for c in collisions)
            
            # Classement par gravité
            severity_counts = defaultdict(int)
            for c in collisions:
                severity = c.get("severity", "Non précisé")
                severity_counts[severity] += 1
            
            correlations.append({
                "period_id": period_id,
                "start_date": period["start_date"],
                "end_date": period["end_date"],
                "weather": {
                    "mean_temp_c": mean_temp,
                    "min_temp_c": min_temp,
                    "max_temp_c": max_temp,
                    "total_precip_mm": total_precip,
                    "total_snow_cm": total_snow,
                },
                "collisions": {
                    "total": n_collisions,
                    "deaths": n_deaths,
                    "severely_injured": n_severe,
                    "lightly_injured": n_light,
                    "by_severity": dict(severity_counts),
                }
            })
        
        # 5. Analyser les tendances par température
        cold_periods = [p for p in correlations if p["weather"]["mean_temp_c"] and p["weather"]["mean_temp_c"] < -10]
        mild_periods = [p for p in correlations if p["weather"]["mean_temp_c"] and -10 <= p["weather"]["mean_temp_c"] <= 0]
        warm_periods = [p for p in correlations if p["weather"]["mean_temp_c"] and p["weather"]["mean_temp_c"] > 0]
        
        temperature_analysis = {}
        if cold_periods:
            avg_cold = sum(p["collisions"]["total"] for p in cold_periods) / len(cold_periods)
            temperature_analysis["cold"] = {
                "threshold": "< -10°C",
                "periods": len(cold_periods),
                "avg_collisions": round(avg_cold, 1)
            }
        
        if mild_periods:
            avg_mild = sum(p["collisions"]["total"] for p in mild_periods) / len(mild_periods)
            temperature_analysis["mild"] = {
                "threshold": "-10°C to 0°C",
                "periods": len(mild_periods),
                "avg_collisions": round(avg_mild, 1)
            }
        
        if warm_periods:
            avg_warm = sum(p["collisions"]["total"] for p in warm_periods) / len(warm_periods)
            temperature_analysis["warm"] = {
                "threshold": "> 0°C",
                "periods": len(warm_periods),
                "avg_collisions": round(avg_warm, 1)
            }
        
        # 6. Analyser les tendances par précipitations
        dry_periods = [p for p in correlations if p["weather"]["total_precip_mm"] < 10]
        wet_periods = [p for p in correlations if 10 <= p["weather"]["total_precip_mm"] < 50]
        very_wet_periods = [p for p in correlations if p["weather"]["total_precip_mm"] >= 50]
        
        precipitation_analysis = {}
        if dry_periods:
            avg_dry = sum(p["collisions"]["total"] for p in dry_periods) / len(dry_periods)
            precipitation_analysis["dry"] = {
                "threshold": "< 10mm",
                "periods": len(dry_periods),
                "avg_collisions": round(avg_dry, 1)
            }
        
        if wet_periods:
            avg_wet = sum(p["collisions"]["total"] for p in wet_periods) / len(wet_periods)
            precipitation_analysis["wet"] = {
                "threshold": "10-50mm",
                "periods": len(wet_periods),
                "avg_collisions": round(avg_wet, 1)
            }
        
        if very_wet_periods:
            avg_very_wet = sum(p["collisions"]["total"] for p in very_wet_periods) / len(very_wet_periods)
            precipitation_analysis["very_wet"] = {
                "threshold": "> 50mm",
                "periods": len(very_wet_periods),
                "avg_collisions": round(avg_very_wet, 1)
            }
        
        # 7. Analyser les tendances par neige
        no_snow = [p for p in correlations if p["weather"]["total_snow_cm"] < 5]
        some_snow = [p for p in correlations if 5 <= p["weather"]["total_snow_cm"] < 20]
        heavy_snow = [p for p in correlations if p["weather"]["total_snow_cm"] >= 20]
        
        snow_analysis = {}
        if no_snow:
            avg_no_snow = sum(p["collisions"]["total"] for p in no_snow) / len(no_snow)
            snow_analysis["no_snow"] = {
                "threshold": "< 5cm",
                "periods": len(no_snow),
                "avg_collisions": round(avg_no_snow, 1)
            }
        
        if some_snow:
            avg_some_snow = sum(p["collisions"]["total"] for p in some_snow) / len(some_snow)
            snow_analysis["some_snow"] = {
                "threshold": "5-20cm",
                "periods": len(some_snow),
                "avg_collisions": round(avg_some_snow, 1)
            }
        
        if heavy_snow:
            avg_heavy_snow = sum(p["collisions"]["total"] for p in heavy_snow) / len(heavy_snow)
            snow_analysis["heavy_snow"] = {
                "threshold": "> 20cm",
                "periods": len(heavy_snow),
                "avg_collisions": round(avg_heavy_snow, 1)
            }
        
        # 8. Top 5 des périodes avec le plus de collisions
        top_periods = sorted(
            correlations,
            key=lambda x: x["collisions"]["total"],
            reverse=True
        )[:5]
        
        # 9. Retourner le résultat structuré
        total_collisions_count = sum(p["collisions"]["total"] for p in correlations)
        
        return {
            "summary": {
                "start_date": start_date,
                "end_date": end_date,
                "frequency": frequency,
                "total_periods": len(correlations),
                "total_collisions": total_collisions_count,
                "avg_collisions_per_period": round(total_collisions_count / len(correlations), 1) if correlations else 0,
            },
            "correlations": correlations,
            "temperature_analysis": temperature_analysis,
            "precipitation_analysis": precipitation_analysis,
            "snow_analysis": snow_analysis,
            "top_periods": top_periods,
        }

class STMBottleneckQuery(DashboardQuery):
    def execute(self, **kwargs) -> Dict:
        # Placeholder for STM bottleneck query implementation
        return {"message": "STM bottleneck query not implemented yet."}

class CollisionHeatMapQuery(DashboardQuery):
    def __init__(self, db_path = DEFAULT_DB_PATH):
        super().__init__(db_path)
        self.severity_mapping = {
            0: "Dommages matériels inférieurs au seuil de rapportage",
            1: "Dommages matériels seulement",
            2: "Léger",
            3: "Grave",
            4: "Mortel"
        }
            
    def execute(self, 
                time_range: str,
                severity_filter: int = None,
                death_nb: int = None,
                severely_injured_nb: int = None,
                lightly_injured_nb: int = None,
                **kwargs) -> Dict:
        """
        Execute the query to get collision data for a heatmap visualization, with optional filters for severity and injury counts.
        Args:
            time_range (str): The time range for filtering collisions (e.g., 'last_month', 'last_week' or a specific date range like '2023-01-01 to 2023-01-31').
            severity_filter (int, optional): Filter for collision severity (0-4). Defaults to None (no filter).
            death_nb (int, optional): Filter for number of deaths. Defaults to None (no filter).
            severely_injured_nb (int, optional): Filter for number of severely injured. Defaults to None (no filter).
            lightly_injured_nb (int, optional): Filter for number of lightly injured. Defaults to None (no filter).
        Information about severity levels:
            0: Dommages matériels inférieurs au seuil de rapportage
            1: Dommages matériels seulement
            2: Léger
            3: Grave
            4: Mortel
        Returns:
            Dict: A dictionary containing the filtered collision data for heatmap visualization.
        Example usage:
            query = CollisionHeatMapQuery()
            result = query.execute(time_range='last_month', severity_filter=4)
        Return format:
            {
                "collisions": [
                    {"lat": 45.5017, "lon": -73.5673, "severity": "Mortel", "deaths": 1, "date": "2023-01-15"},
                    ...
                ],
                "total_count": 150
            }
        """
        self.connect()
        cursor = self.conn.cursor()
        logger.debug(f"Executing CollisionHeatMapQuery with time_range='{time_range}', severity_filter={severity_filter}, death_nb={death_nb}, severely_injured_nb={severely_injured_nb}, lightly_injured_nb={lightly_injured_nb}")
        
        # Parse time_range to get start and end dates
        start_date, end_date = parse_time_range(time_range)

        query = """
            SELECT DT_ACCDN, GRAVITE, NB_MORTS, NB_BLESSES_GRAVES, NB_BLESSES_LEGERS, 
                   LOC_LAT, LOC_LONG, NO_SEQ_COLL
            FROM collisions_routieres
            WHERE DT_ACCDN IS NOT NULL
              AND DT_ACCDN >= ?
              AND DT_ACCDN <= ?
              AND LOC_LAT IS NOT NULL
              AND LOC_LONG IS NOT NULL
        """
        params = [start_date, end_date]
        
        # Add optional filters
        if severity_filter is not None:
            severity_text = self.severity_mapping.get(severity_filter)
            if severity_text:
                query += " AND GRAVITE = ?"
                params.append(severity_text)
        
        if death_nb is not None:
            query += " AND CAST(NB_MORTS AS INTEGER) >= ?"
            params.append(death_nb)
        
        if severely_injured_nb is not None:
            query += " AND CAST(NB_BLESSES_GRAVES AS INTEGER) >= ?"
            params.append(severely_injured_nb)
        
        if lightly_injured_nb is not None:
            query += " AND CAST(NB_BLESSES_LEGERS AS INTEGER) >= ?"
            params.append(lightly_injured_nb)
        
        cursor.execute(query, params)
        rows = cursor.fetchall()
        logger.debug(f"Executed SQL query: {query} with params {params}")
        logger.debug(f"Total collisions retrieved from database: {len(rows)}")
        self.disconnect()
        
        # Process results
        collisions = []
        for row in rows:
            dt_accdn, gravite, nb_morts, nb_blesses_graves, nb_blesses_legers, loc_lat, loc_long, no_seq = row
            
            # Convert coordinates to float
            try:
                lat = float(loc_lat)
                lon = float(loc_long)
                # Validate coordinates are in Montreal area (approx 45°N, -73°W)
                # Montreal spans roughly -74.5°W to -71.5°W, 44.5°N to 45.8°N
                if not (44 <= lat <= 46 and -75 <= lon <= -71):
                    continue
            except (ValueError, TypeError):
                continue
            
            # Convert counts to int with default 0
            try:
                deaths = int(nb_morts) if nb_morts else 0
                severely_injured = int(nb_blesses_graves) if nb_blesses_graves else 0
                lightly_injured = int(nb_blesses_legers) if nb_blesses_legers else 0
            except (ValueError, TypeError):
                deaths = 0
                severely_injured = 0
                lightly_injured = 0
            
            # Convert date to string and id
            try:
                date_str = str(dt_accdn) if dt_accdn else ""
                # NO_SEQ_COLL is already a string like "SPVM _ 2021 _ 10370"
                collision_id = str(no_seq) if no_seq else ""
            except (ValueError, TypeError):
                continue
            
            collisions.append({
                "lat": lat,
                "lon": lon,
                "severity": gravite if gravite else "Non précisé",
                "deaths": deaths,
                "severely_injured": severely_injured,
                "lightly_injured": lightly_injured,
                "date": date_str,
                "id": collision_id
            })
        
        return {
            "collisions": collisions,
            "total_count": len(collisions)
        }


if __name__ == "__main__":
    # Test WordCloudQuery311
    print("=== WordCloudQuery311 ===")
    query = WordCloudQuery311(db_path="db/mobility.db")
    result = query.execute(top_n=10, time_range='last_month')
    print(json.dumps(result, indent=2, ensure_ascii=False))
    
    # Test CollisionHeatMapQuery
    print("\n=== CollisionHeatMapQuery ===")
    query2 = CollisionHeatMapQuery(db_path="db/mobility.db")
    result2 = query2.execute(time_range='2020-01-01 to 2020-12-31', severity_filter=4)
    print(f"Total collisions mortelles en 2020: {result2['total_count']}")
    if result2['collisions']:
        print("Premier exemple:")
        print(json.dumps(result2['collisions'][0], indent=2, ensure_ascii=False))
    
    # Test WeatherCorrelationQuery
    print("\n=== WeatherCorrelationQuery ===")
    query3 = WeatherCorrelationQuery(db_path="db/mobility.db")
    result3 = query3.execute(
        start_date='2021-01-01',
        end_date='2021-01-31',
        frequency='week'
    )
    if 'error' in result3:
        print(f"Erreur: {result3['error']}")
    else:
        print(f"Périodes analysées: {result3['summary']['total_periods']}")
        print(f"Total collisions: {result3['summary']['total_collisions']}")
        print(f"Moyenne par période: {result3['summary']['avg_collisions_per_period']}")
        print("\nAnalyse température:")
        for key, data in result3.get('temperature_analysis', {}).items():
            print(f"  {key}: {data['periods']} périodes, moy {data['avg_collisions']} collisions ({data['threshold']})")
        if result3['top_periods']:
            print("\nTop période:")
            top = result3['top_periods'][0]
            print(f"  {top['period_id']}: {top['collisions']['total']} collisions")
            print(f"  Température moyenne: {top['weather']['mean_temp_c']:.1f}°C")