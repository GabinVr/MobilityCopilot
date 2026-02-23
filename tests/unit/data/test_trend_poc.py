import json
import sqlite3

import pytest

from data.trend import TrendQuery


def _seed_poc_database(db_path: str) -> None:
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE collisions_routieres (
            DT_ACCDN TEXT,
            HEURE_ACCDN TEXT,
            GRAVITE TEXT,
            NB_VICTIMES_PIETON TEXT
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE requetes311 (
            DDS_DATE_CREATION TEXT,
            ACTI_NOM TEXT,
            ARRONDISSEMENT TEXT
        )
        """
    )

    collisions_rows = [
        # Previous 8-week window (peak hour expected around 17h)
        ("2024-01-15", "17:00", "Grave", "0"),
        ("2024-01-20", "17:15", "Léger", "0"),
        ("2024-02-01", "17:30", "Léger", "1"),
        ("2024-02-10", "18:00", "Léger", "0"),
        # Recent 8-week window (peak hour expected around 15h)
        ("2024-03-10", "15:10", "Léger", "1"),
        ("2024-03-12", "15:20", "Léger", "1"),
        ("2024-04-01", "15:30", "Grave", "1"),
        ("2024-04-10", "16:00", "Léger", "0"),
        # Same period previous year for 3m-vs-last-year comparison
        ("2023-02-15", "17:00", "Léger", "1"),
        ("2023-03-20", "18:00", "Léger", "1"),
    ]
    cur.executemany(
        """
        INSERT INTO collisions_routieres (DT_ACCDN, HEURE_ACCDN, GRAVITE, NB_VICTIMES_PIETON)
        VALUES (?, ?, ?, ?)
        """,
        collisions_rows,
    )

    # Weekly increasing signal for one 311 activity (Aqueduc/fuite)
    weekly_counts = [
        ("2024-03-04", "Aqueduc/fuite", 1),
        ("2024-03-11", "Aqueduc/fuite", 2),
        ("2024-03-18", "Aqueduc/fuite", 3),
        ("2024-03-25", "Aqueduc/fuite", 4),
        ("2024-04-01", "Aqueduc/fuite", 5),
        ("2024-04-08", "Aqueduc/fuite", 6),
        ("2024-04-15", "Aqueduc/fuite", 7),
        ("2024-04-22", "Aqueduc/fuite", 8),
    ]
    for base_date, activity, count in weekly_counts:
        for _ in range(count):
            cur.execute(
                """
                INSERT INTO requetes311 (DDS_DATE_CREATION, ACTI_NOM, ARRONDISSEMENT)
                VALUES (?, ?, ?)
                """,
                (base_date, activity, "Ville-Marie"),
            )

    # Stable activity for contrast
    for base_date in [
        "2024-03-04",
        "2024-03-11",
        "2024-03-18",
        "2024-03-25",
        "2024-04-01",
        "2024-04-08",
        "2024-04-15",
        "2024-04-22",
    ]:
        for _ in range(3):
            cur.execute(
                """
                INSERT INTO requetes311 (DDS_DATE_CREATION, ACTI_NOM, ARRONDISSEMENT)
                VALUES (?, ?, ?)
                """,
                (base_date, "Nids-de-poule", "Le Plateau-Mont-Royal"),
            )

    conn.commit()
    conn.close()


def _print_section(title: str, payload):
    print(f"\n=== {title} ===")
    print(json.dumps(payload, indent=2, ensure_ascii=False, default=str))


@pytest.fixture
def trend_report(tmp_path):
    db_path = tmp_path / "trend_poc.db"
    _seed_poc_database(str(db_path))

    query = TrendQuery(db_path=str(db_path))
    return query.execute(as_of_date="2024-04-28")


def test_report_structure_is_complete(trend_report):
    _print_section("report/meta", {"as_of_date": trend_report["as_of_date"], "generated_at": trend_report["generated_at"]})

    required_keys = [
        "monthly_collisions",
        "pedestrian_3m_vs_last_year",
        "hourly_peak_shift",
        "weekly_311_changes",
        "weak_signals_311",
    ]
    for key in required_keys:
        assert key in trend_report, f"Missing key in report: {key}"


def test_monthly_collision_trend_is_stable_between_march_and_april(trend_report):
    monthly = trend_report["monthly_collisions"]
    _print_section("monthly_collisions", monthly)

    assert monthly["current_period"] == "2024-04", f"Unexpected current period: {monthly['current_period']}"
    assert monthly["previous_period"] == "2024-03", f"Unexpected previous period: {monthly['previous_period']}"
    assert monthly["direction"] == "stable", f"Expected stable, got {monthly['direction']}"


def test_pedestrian_collisions_are_up_vs_previous_year(trend_report):
    pedestrian = trend_report["pedestrian_3m_vs_last_year"]
    _print_section("pedestrian_3m_vs_last_year", pedestrian)

    assert pedestrian["direction"] == "up", f"Expected up, got {pedestrian['direction']}"
    assert pedestrian["current_period"]["count"] > pedestrian["comparison_period"]["count"], (
        "Current period count should be greater than comparison period count "
        f"({pedestrian['current_period']['count']} <= {pedestrian['comparison_period']['count']})"
    )


def test_peak_hour_shift_moves_from_17h_to_15h(trend_report):
    peak = trend_report["hourly_peak_shift"]
    _print_section("hourly_peak_shift", peak)

    assert peak["recent_window"]["peak_hour"] == 15, f"Expected recent peak at 15h, got {peak['recent_window']['peak_hour']}"
    assert peak["previous_window"]["peak_hour"] == 17, (
        f"Expected previous peak at 17h, got {peak['previous_window']['peak_hour']}"
    )
    assert peak["shift_hours"] == -2, f"Expected shift -2h, got {peak['shift_hours']}"


def test_311_changes_and_weak_signals_expose_aqueduc_fuite(trend_report):
    weekly_changes = trend_report["weekly_311_changes"]["changes"]
    weak_signals = trend_report["weak_signals_311"]["signals"]
    _print_section("weekly_311_changes", trend_report["weekly_311_changes"])
    _print_section("weak_signals_311", trend_report["weak_signals_311"])

    assert any(item["activity"] == "Aqueduc/fuite" for item in weekly_changes), (
        "Aqueduc/fuite should appear in weekly_311_changes."
    )
    assert any(item["activity"] == "Aqueduc/fuite" for item in weak_signals), (
        "Aqueduc/fuite should appear in weak_signals_311."
    )
