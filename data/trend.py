import json
import os
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from data.dashboard_queries import DashboardQuery

LOCAL_DIR = Path(__file__).parent
DEFAULT_DB_PATH = os.path.join(LOCAL_DIR, "db/mobility.db")


def _safe_pct_change(current: float, previous: float) -> Optional[float]:
    if previous == 0:
        return None
    return round(((current - previous) / previous) * 100, 1)


def _direction_from_diff(diff: float) -> str:
    if diff > 0:
        return "up"
    if diff < 0:
        return "down"
    return "stable"


class TrendQuery(DashboardQuery):
    """
    Trend analyzer implementing the DashboardQuery interface.
    """

    def __init__(self, db_path: str = DEFAULT_DB_PATH):
        super().__init__(db_path)
        self.collisions_table = self._pick_table(["collisions_routieres", "collisions"])
        self.requests_table = self._pick_table(["requetes311", "demandes"])

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.db_path)

    def _pick_table(self, candidates: List[str]) -> str:
        with self._connect() as conn:
            cur = conn.cursor()
            for table in candidates:
                cur.execute(
                    "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ? LIMIT 1",
                    (table,),
                )
                if cur.fetchone():
                    return table
        raise ValueError(f"No table found among: {', '.join(candidates)}")

    def _read_sql(self, query: str, params: Tuple[Any, ...] = ()) -> pd.DataFrame:
        with self._connect() as conn:
            return pd.read_sql_query(query, conn, params=params)

    @staticmethod
    def _normalize_date_series(series: pd.Series) -> pd.Series:
        # Supports YYYY-MM-DD and YYYY/MM/DD formats and datetime strings.
        normalized = (
            series.astype(str)
            .str.strip()
            .str.replace("/", "-", regex=False)
            .replace({"": np.nan, "None": np.nan, "nan": np.nan})
        )
        return pd.to_datetime(normalized, errors="coerce")

    @staticmethod
    def _to_numeric(series: pd.Series) -> pd.Series:
        return pd.to_numeric(series, errors="coerce").fillna(0)

    def _load_collisions(self) -> pd.DataFrame:
        df = self._read_sql(
            f"""
            SELECT DT_ACCDN, HEURE_ACCDN, GRAVITE, NB_VICTIMES_PIETON
            FROM {self.collisions_table}
            WHERE DT_ACCDN IS NOT NULL
            """
        )
        if df.empty:
            return df
        df["date"] = self._normalize_date_series(df["DT_ACCDN"])
        df = df.dropna(subset=["date"]).copy()
        if "NB_VICTIMES_PIETON" in df.columns:
            df["NB_VICTIMES_PIETON"] = self._to_numeric(df["NB_VICTIMES_PIETON"])
        else:
            df["NB_VICTIMES_PIETON"] = 0
        return df

    def _load_requests_311(self) -> pd.DataFrame:
        df = self._read_sql(
            f"""
            SELECT DDS_DATE_CREATION, ACTI_NOM, ARRONDISSEMENT
            FROM {self.requests_table}
            WHERE DDS_DATE_CREATION IS NOT NULL
            """
        )
        if df.empty:
            return df
        df["date"] = self._normalize_date_series(df["DDS_DATE_CREATION"])
        df = df.dropna(subset=["date"]).copy()
        df["ACTI_NOM"] = (
            df["ACTI_NOM"]
            .astype(str)
            .str.strip()
            .replace({"": "Non précise", "nan": "Non precise", "None": "Non precise"})
        )
        return df

    def _resolve_as_of_date(
        self,
        collisions_df: pd.DataFrame,
        requests_df: pd.DataFrame,
        as_of_date: Optional[str] = None,
    ) -> pd.Timestamp:
        if as_of_date:
            parsed = pd.to_datetime(as_of_date, errors="coerce")
            if pd.isna(parsed):
                raise ValueError("Invalid as_of_date format. Expected YYYY-MM-DD.")
            return parsed.normalize()

        candidates: List[pd.Timestamp] = []
        if not collisions_df.empty:
            candidates.append(collisions_df["date"].max().normalize())
        if not requests_df.empty:
            candidates.append(requests_df["date"].max().normalize())
        if not candidates:
            return pd.Timestamp.now().normalize()
        return max(candidates)

    def monthly_collision_trend(
        self,
        collisions_df: pd.DataFrame,
        as_of: pd.Timestamp,
        months: int = 12,
    ) -> Dict[str, Any]:
        if collisions_df.empty:
            return {"series": [], "message": "No collision data available."}

        start = (as_of - pd.DateOffset(months=months - 1)).replace(day=1)
        scope = collisions_df[collisions_df["date"] >= start].copy()
        if scope.empty:
            return {"series": [], "message": "No collision data in selected period."}

        scope["month"] = scope["date"].dt.to_period("M").astype(str)
        grouped = scope.groupby("month").size().sort_index()

        series = [
            {"period": period, "count": int(count)}
            for period, count in grouped.items()
        ]

        if len(grouped) < 2:
            return {
                "series": series,
                "current_period": grouped.index[-1],
                "current_count": int(grouped.iloc[-1]),
                "previous_period": None,
                "previous_count": None,
                "diff": None,
                "pct_change": None,
                "direction": "stable",
            }

        current_period = grouped.index[-1]
        previous_period = grouped.index[-2]
        current_count = int(grouped.iloc[-1])
        previous_count = int(grouped.iloc[-2])
        diff = current_count - previous_count

        return {
            "series": series,
            "current_period": current_period,
            "current_count": current_count,
            "previous_period": previous_period,
            "previous_count": previous_count,
            "diff": diff,
            "pct_change": _safe_pct_change(current_count, previous_count),
            "direction": _direction_from_diff(diff),
        }

    def pedestrian_3m_vs_last_year(
        self,
        collisions_df: pd.DataFrame,
        as_of: pd.Timestamp,
    ) -> Dict[str, Any]:
        if collisions_df.empty:
            return {"message": "No collision data available."}

        current_start = (as_of - pd.DateOffset(months=3)).normalize()
        current_end = as_of.normalize()

        previous_start = (current_start - pd.DateOffset(years=1)).normalize()
        previous_end = (current_end - pd.DateOffset(years=1)).normalize()

        with_pedestrian = collisions_df[collisions_df["NB_VICTIMES_PIETON"] > 0]

        current_count = int(
            with_pedestrian[
                (with_pedestrian["date"] >= current_start)
                & (with_pedestrian["date"] <= current_end)
            ].shape[0]
        )
        previous_count = int(
            with_pedestrian[
                (with_pedestrian["date"] >= previous_start)
                & (with_pedestrian["date"] <= previous_end)
            ].shape[0]
        )
        diff = current_count - previous_count

        return {
            "metric": "pedestrian_collisions",
            "current_period": {
                "start": current_start.strftime("%Y-%m-%d"),
                "end": current_end.strftime("%Y-%m-%d"),
                "count": current_count,
            },
            "comparison_period": {
                "start": previous_start.strftime("%Y-%m-%d"),
                "end": previous_end.strftime("%Y-%m-%d"),
                "count": previous_count,
            },
            "diff": diff,
            "pct_change": _safe_pct_change(current_count, previous_count),
            "direction": _direction_from_diff(diff),
        }

    @staticmethod
    def _extract_hour(value: Any) -> Optional[int]:
        if value is None:
            return None
        text = str(value).strip()
        if not text:
            return None
        match = re.search(r"(\d{1,2})", text)
        if not match:
            return None
        hour = int(match.group(1))
        if 0 <= hour <= 23:
            return hour
        return None

    def hourly_peak_shift(
        self,
        collisions_df: pd.DataFrame,
        as_of: pd.Timestamp,
        window_weeks: int = 8,
    ) -> Dict[str, Any]:
        if collisions_df.empty:
            return {"message": "No collision data available."}

        df = collisions_df.copy()
        df["hour"] = df["HEURE_ACCDN"].apply(self._extract_hour)
        df = df.dropna(subset=["hour"]).copy()
        if df.empty:
            return {"message": "No hour data available in collisions."}

        recent_start = (as_of - pd.Timedelta(weeks=window_weeks)).normalize()
        previous_end = recent_start - pd.Timedelta(days=1)
        previous_start = (previous_end - pd.Timedelta(weeks=window_weeks)).normalize()

        recent = df[(df["date"] >= recent_start) & (df["date"] <= as_of)]
        previous = df[(df["date"] >= previous_start) & (df["date"] <= previous_end)]

        if recent.empty or previous.empty:
            return {
                "window_weeks": window_weeks,
                "message": "Insufficient data for peak-hour shift.",
            }

        recent_counts = recent.groupby("hour").size()
        prev_counts = previous.groupby("hour").size()

        recent_peak = int(recent_counts.idxmax())
        previous_peak = int(prev_counts.idxmax())
        shift = recent_peak - previous_peak

        return {
            "window_weeks": window_weeks,
            "recent_window": {
                "start": recent_start.strftime("%Y-%m-%d"),
                "end": as_of.strftime("%Y-%m-%d"),
                "peak_hour": recent_peak,
                "peak_count": int(recent_counts.max()),
            },
            "previous_window": {
                "start": previous_start.strftime("%Y-%m-%d"),
                "end": previous_end.strftime("%Y-%m-%d"),
                "peak_hour": previous_peak,
                "peak_count": int(prev_counts.max()),
            },
            "shift_hours": int(shift),
            "direction": _direction_from_diff(shift),
        }

    def weekly_311_top_changes(
        self,
        requests_df: pd.DataFrame,
        as_of: pd.Timestamp,
        weeks: int = 10,
        top_n: int = 8,
    ) -> Dict[str, Any]:
        if requests_df.empty:
            return {"changes": [], "message": "No 311 data available."}

        start = (as_of - pd.Timedelta(weeks=weeks)).normalize()
        scope = requests_df[(requests_df["date"] >= start) & (requests_df["date"] <= as_of)].copy()
        if scope.empty:
            return {"changes": [], "message": "No 311 data in selected period."}

        scope["week_start"] = scope["date"].dt.to_period("W-MON").apply(lambda p: p.start_time)
        grouped = (
            scope.groupby(["ACTI_NOM", "week_start"])
            .size()
            .reset_index(name="count")
        )
        if grouped.empty:
            return {"changes": [], "message": "No grouped 311 data."}

        weeks_sorted = sorted(grouped["week_start"].unique())
        if len(weeks_sorted) < 2:
            return {"changes": [], "message": "At least 2 weeks are required for comparison."}

        recent_week = weeks_sorted[-1]
        previous_week = weeks_sorted[-2]

        pivot = grouped.pivot(
            index="ACTI_NOM",
            columns="week_start",
            values="count",
        ).fillna(0)
        pivot["recent"] = pivot.get(recent_week, 0)
        pivot["previous"] = pivot.get(previous_week, 0)
        pivot["diff"] = pivot["recent"] - pivot["previous"]
        pivot["pct_change"] = pivot.apply(
            lambda row: _safe_pct_change(float(row["recent"]), float(row["previous"])),
            axis=1,
        )
        pivot = pivot.sort_values("diff", ascending=False)

        changes = []
        for activity, row in pivot.head(top_n).iterrows():
            diff = int(row["diff"])
            pct_change = row["pct_change"]
            if pct_change is not None:
                pct_change = float(pct_change)
            changes.append(
                {
                    "activity": str(activity),
                    "recent_week_count": int(row["recent"]),
                    "previous_week_count": int(row["previous"]),
                    "diff": diff,
                    "pct_change": pct_change,
                    "direction": _direction_from_diff(diff),
                }
            )

        return {
            "recent_week_start": pd.Timestamp(recent_week).strftime("%Y-%m-%d"),
            "previous_week_start": pd.Timestamp(previous_week).strftime("%Y-%m-%d"),
            "changes": changes,
        }

    def weak_signals_311(
        self,
        requests_df: pd.DataFrame,
        as_of: pd.Timestamp,
        weeks: int = 6,
        max_weekly_avg: float = 20.0,
        min_positive_steps: int = 4,
        top_n: int = 10,
    ) -> Dict[str, Any]:
        if requests_df.empty:
            return {"signals": [], "message": "No 311 data available."}

        start = (as_of - pd.Timedelta(weeks=weeks)).normalize()
        scope = requests_df[(requests_df["date"] >= start) & (requests_df["date"] <= as_of)].copy()
        if scope.empty:
            return {"signals": [], "message": "No 311 data in selected period."}

        scope["week_start"] = scope["date"].dt.to_period("W-MON").apply(lambda p: p.start_time)
        week_axis = sorted(scope["week_start"].unique())
        if len(week_axis) < 4:
            return {"signals": [], "message": "At least 4 weeks are required for weak signals."}

        grouped = (
            scope.groupby(["ACTI_NOM", "week_start"])
            .size()
            .reset_index(name="count")
        )
        pivot = grouped.pivot(index="ACTI_NOM", columns="week_start", values="count").fillna(0)
        pivot = pivot.reindex(columns=week_axis, fill_value=0)

        signals: List[Dict[str, Any]] = []
        x = np.arange(len(week_axis))
        for activity, row in pivot.iterrows():
            y = row.values.astype(float)
            avg = float(np.mean(y))
            if avg > max_weekly_avg:
                continue

            deltas = np.diff(y)
            positive_steps = int(np.sum(deltas > 0))
            if positive_steps < min_positive_steps:
                continue
            if y[-1] <= y[0]:
                continue

            slope = float(np.polyfit(x, y, 1)[0]) if len(y) > 1 else 0.0
            signals.append(
                {
                    "activity": str(activity),
                    "weekly_counts": [int(v) for v in y],
                    "average_per_week": round(avg, 2),
                    "increase_steps": positive_steps,
                    "start_count": int(y[0]),
                    "end_count": int(y[-1]),
                    "slope": round(slope, 3),
                }
            )

        signals.sort(key=lambda item: item["slope"], reverse=True)
        return {
            "window_weeks": weeks,
            "signals": signals[:top_n],
        }

    def execute(self, as_of_date: Optional[str] = None, **kwargs) -> Dict[str, Any]:
        collisions_df = self._load_collisions()
        requests_df = self._load_requests_311()
        as_of = self._resolve_as_of_date(collisions_df, requests_df, as_of_date=as_of_date)

        monthly = self.monthly_collision_trend(collisions_df=collisions_df, as_of=as_of)
        pedestrian = self.pedestrian_3m_vs_last_year(collisions_df=collisions_df, as_of=as_of)
        peak_shift = self.hourly_peak_shift(collisions_df=collisions_df, as_of=as_of)
        top_311 = self.weekly_311_top_changes(requests_df=requests_df, as_of=as_of)
        weak_signals = self.weak_signals_311(requests_df=requests_df, as_of=as_of)

        insights: List[str] = []
        if "pct_change" in monthly and monthly.get("pct_change") is not None:
            insights.append(
                f"Collisions: {monthly['current_period']} vs {monthly['previous_period']} = "
                f"{monthly['pct_change']}% ({monthly['direction']})."
            )
        if "pct_change" in pedestrian and pedestrian.get("pct_change") is not None:
            insights.append(
                "Collisions pietons (3 mois vs meme periode N-1): "
                f"{pedestrian['pct_change']}% ({pedestrian['direction']})."
            )
        if "shift_hours" in peak_shift and peak_shift.get("shift_hours") is not None:
            insights.append(
                "Pic horaire des collisions: "
                f"{peak_shift['previous_window']['peak_hour']}h -> "
                f"{peak_shift['recent_window']['peak_hour']}h "
                f"(decalage {peak_shift['shift_hours']}h)."
            )
        if weak_signals.get("signals"):
            first_signal = weak_signals["signals"][0]
            insights.append(
                f"Signal faible 311: '{first_signal['activity']}' en hausse "
                f"({first_signal['start_count']} -> {first_signal['end_count']} sur {weak_signals['window_weeks']} semaines)."
            )

        return {
            "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "as_of_date": as_of.strftime("%Y-%m-%d"),
            "monthly_collisions": monthly,
            "pedestrian_3m_vs_last_year": pedestrian,
            "hourly_peak_shift": peak_shift,
            "weekly_311_changes": top_311,
            "weak_signals_311": weak_signals,
            "insights": insights,
        }


if __name__ == "__main__":
    query = TrendQuery()
    report = query.execute()
    print(json.dumps(report, indent=2, ensure_ascii=False))
