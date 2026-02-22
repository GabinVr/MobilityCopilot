import os
import pandas as pd
import numpy as np
import json
from abc import ABC, abstractmethod
from typing import Dict, Any, List
import sqlite3
import re

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
    def __init__(self, db_path: str = "./db/mobility.db"):
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
    def __init__(self, db_path: str = "./db/mobility.db"):
        super().__init__(db_path)
        self.not_allowed_words = set(["des", "d", "de"])
    
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
        df_requests['ACTI_NOM'] = df_requests['ACTI_NOM'].apply(lambda x: x.encode('utf-8', errors='ignore').decode('utf-8', errors='ignore') if isinstance(x, str) else x)
        df_requests['DDS_DATE_CREATION'] = pd.to_datetime(df_requests['DDS_DATE_CREATION'])
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
    def execute(self, **kwargs) -> Dict:
        # Placeholder for weather correlation query implementation
        return {"message": "Weather correlation query not implemented yet."}
    
class STMBottleneckQuery(DashboardQuery):
    def execute(self, **kwargs) -> Dict:
        # Placeholder for STM bottleneck query implementation
        return {"message": "STM bottleneck query not implemented yet."}

class CollisionHeatMapQuery(DashboardQuery):
    def __init__(self, db_path = "./db/mobility.db"):
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
        
        # Parse time_range to get start and end dates
        start_date, end_date = parse_time_range(time_range)
        
        # Convert dates to YYYY/MM/DD format for SQLite comparison
        # The database stores dates as YYYY/MM/DD strings
        start_date_db = start_date.replace('-', '/')
        end_date_db = end_date.replace('-', '/')
        
        # Build SQL query with filters
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
        params = [start_date_db, end_date_db]
        
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
                if not (44 <= lat <= 46 and -74 <= lon <= -72):
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
            
            collisions.append({
                "lat": lat,
                "lon": lon,
                "severity": gravite if gravite else "Non précisé",
                "deaths": deaths,
                "severely_injured": severely_injured,
                "lightly_injured": lightly_injured,
                "date": dt_accdn,
                "id": no_seq
            })
        
        return {
            "collisions": collisions,
            "total_count": len(collisions)
        }


if __name__ == "__main__":
    # Test WordCloudQuery311
    print("=== WordCloudQuery311 ===")
    query = WordCloudQuery311(db_path="data/db/mobility.db")
    result = query.execute(top_n=10, time_range='last_month')
    print(json.dumps(result, indent=2, ensure_ascii=False))
    
    # Test CollisionHeatMapQuery
    print("\n=== CollisionHeatMapQuery ===")
    query2 = CollisionHeatMapQuery(db_path="data/db/mobility.db")
    result2 = query2.execute(time_range='2023-01-01 to 2023-12-31', severity_filter=4)
    print(f"Total collisions mortelles en 2023: {result2['total_count']}")
    if result2['collisions']:
        print("Premier exemple:")
        print(json.dumps(result2['collisions'][0], indent=2, ensure_ascii=False))