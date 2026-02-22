import os
import pandas as pd
import numpy as np
import json
from abc import ABC, abstractmethod
from typing import Dict, Any, List
import sqlite3
import re

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

    def _parse_time_range(self, time_range: str) -> tuple[str, str]:
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
        
        return start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')
    
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
        start_date, end_date = self._parse_time_range(time_range)
        # Execute SQL query to get the relevant service requests
        cursor.execute("""
            SELECT ACTI_NOM, DDS_DATE_CREATION FROM demandes
        """)
        rows = cursor.fetchall()
        self.disconnect()
        # Filter rows based on the time range
        # DDS_DATE_CREATION is in the format 'YYYY-MM-DD HH:MM:SS'
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
    def execute(self, **kwargs) -> Dict:
        # Placeholder for collision heatmap query implementation
        return {"message": "Collision heatmap query not implemented yet."}

if __name__ == "__main__":
    query = WordCloudQuery311()
    result = query.execute(top_n=30, time_range='last_month')
    print(json.dumps(result, indent=2, ensure_ascii=False))