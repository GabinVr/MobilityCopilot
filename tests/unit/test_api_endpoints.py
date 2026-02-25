import pytest
from unittest.mock import Mock, patch, MagicMock, AsyncMock
from fastapi.testclient import TestClient
from main import api
from routes.chat import chat_router
from routes.hotspot import hotspot_router
from services.weekly_report import get_last_hotspot_report, hebdo_hotspots_briefing_generator

from models import (    
    ChatRequest,
    ChatResponse,
    WordCloudRequest,
    WordCloudResponse,
    CollisionHeatMapRequest,
    CollisionHeatMapResponse,
    WeatherCorrelationRequest,
    WeatherCorrelationResponse,
    TrendRequest,
    TrendResponse,
    CollisionForecastJ1Request,
    CollisionForecastJ1Response,
)


@pytest.fixture
def client():
    """Créer un client de test pour l'API FastAPI."""
    # Mock le graph qui est normalement créé dans le lifespan
    mock_graph = MagicMock()
    
    # Créer le client sans le lifespan
    with patch('main.lifespan'):
        from main import api
        # Ajouter le mock du graph à l'état de l'app
        api.state.graph = mock_graph
        client = TestClient(api)
        yield client


class TestChatEndpoint:
    """Tests pour l'endpoint /chat."""
    
    def test_chat_endpoint_success(self, client):
        """Tester l'endpoint /chat avec une requête valide."""
        payload = {
            "query": "Quel est le nombre de collisions ce mois-ci?",
            "audience": "grand_public"
        }
        
        client.app.state.graph.invoke = AsyncMock(return_value={
            "messages": [],
            "audience": "grand_public",
            "is_ambiguous": False,
            "analytical_response": "Il y a eu 150 collisions ce mois-ci."
        })
        
        response = client.post("/chat", json=payload)
        
        assert response.status_code == 200
        data = response.json()
        assert data["answer"] == "Il y a eu 150 collisions ce mois-ci."
        assert data["is_ambiguous"] is False
    
    def test_chat_endpoint_ambiguous(self, client):
        """Tester l'endpoint /chat avec une requête ambiguë."""
        payload = {
            "query": "Parlez-moi des incidents",
            "audience": "municipalite"
        }
        
        client.app.state.graph.invoke = AsyncMock(return_value={
            "messages": [],
            "is_ambiguous": True,
            "clarification_options": "Voulez-vous parler des collisions, des requêtes 311, ou des perturbations du transport?"
        })
        
        response = client.post("/chat", json=payload)
        
        assert response.status_code == 200
        data = response.json()
        assert data["is_ambiguous"] is True
        assert "Voulez-vous parler" in data["answer"]
    
    def test_chat_endpoint_with_contradictor_notes(self, client):
        """Tester l'endpoint /chat avec des notes du contradictor."""
        payload = {
            "query": "Les collisions augmentent?",
            "audience": "grand_public"
        }
        
        client.app.state.graph.invoke = AsyncMock(return_value={
            "messages": [],
            "is_ambiguous": False,
            "analytical_response": "Selon les données, les collisions ont augmenté de 10%.",
            "contradictor_notes": "Attention: les données du mois dernier sont incomplètes."
        })
        
        response = client.post("/chat", json=payload)
        
        assert response.status_code == 200
        data = response.json()
        assert data["contradictor_notes"] == "Attention: les données du mois dernier sont incomplètes."
    
    def test_chat_endpoint_error(self, client):
        """Tester l'endpoint /chat avec une erreur."""
        payload = {
            "query": "Test query",
            "audience": "grand_public"
        }
        
        client.app.state.graph.invoke = AsyncMock(side_effect=Exception("Erreur du langgraph"))
        
        response = client.post("/chat", json=payload)
        
        assert response.status_code == 500


class TestWordCloudEndpoint:
    """Tests pour l'endpoint /dashboard/wordcloud-311."""
    
    def test_wordcloud_endpoint_success(self, client):
        """Tester l'endpoint wordcloud avec des résultats valides."""
        payload = {
            "top_n": 10,
            "time_range": "last_month"
        }
        
        with patch('routes.wordcloud.WordCloudQuery311') as mock_query_class:
            mock_query = MagicMock()
            mock_query_class.return_value = mock_query
            mock_query.execute.return_value = {
                "top_words": [
                    {"word": "pothole", "count": 150},
                    {"word": "graffiti", "count": 120},
                    {"word": "light", "count": 95}
                ]
            }
            
            response = client.post("/dashboard/wordcloud-311", json=payload)
            
            assert response.status_code == 200
            data = response.json()
            assert len(data["top_words"]) == 3
            assert data["top_words"][0]["word"] == "pothole"
            assert data["top_words"][0]["count"] == 150
    
    def test_wordcloud_endpoint_default_params(self, client):
        """Tester l'endpoint wordcloud avec les paramètres par défaut."""
        payload = {}
        
        with patch('routes.wordcloud.WordCloudQuery311') as mock_query_class:
            mock_query = MagicMock()
            mock_query_class.return_value = mock_query
            mock_query.execute.return_value = {
                "top_words": []
            }
            
            response = client.post("/dashboard/wordcloud-311", json=payload)
            
            assert response.status_code == 200
            mock_query.execute.assert_called_once_with(top_n=10, time_range="last_month")
    
    def test_wordcloud_endpoint_date_range(self, client):
        """Tester l'endpoint wordcloud avec une plage de dates."""
        payload = {
            "top_n": 20,
            "time_range": "2023-01-01 to 2023-01-31"
        }
        
        with patch('routes.wordcloud.WordCloudQuery311') as mock_query_class:
            mock_query = MagicMock()
            mock_query_class.return_value = mock_query
            mock_query.execute.return_value = {
                "top_words": [{"word": "test", "count": 50}]
            }
            
            response = client.post("/dashboard/wordcloud-311", json=payload)
            
            assert response.status_code == 200
            mock_query.execute.assert_called_once_with(top_n=20, time_range="2023-01-01 to 2023-01-31")


class TestCollisionHeatMapEndpoint:
    """Tests pour l'endpoint /dashboard/collision-heatmap."""
    
    def test_collision_heatmap_endpoint_success(self, client):
        """Tester l'endpoint heatmap avec des résultats valides."""
        payload = {
            "time_range": "last_month"
        }
        
        with patch('routes.collision_heatmap.CollisionHeatMapQuery') as mock_query_class:
            mock_query = MagicMock()
            mock_query_class.return_value = mock_query
            mock_query.execute.return_value = {
                "collisions": [
                    {
                        "lat": 45.5017,
                        "lon": -73.5673,
                        "severity": "Grave",
                        "deaths": 1,
                        "severely_injured": 3,
                        "lightly_injured": 5,
                        "date": "2023-01-15",
                        "id": "COL001"
                    }
                ],
                "total_count": 1
            }
            
            response = client.post("/dashboard/collision-heatmap", json=payload)
            
            assert response.status_code == 200
            data = response.json()
            assert data["total_count"] == 1
            assert len(data["collisions"]) == 1
            assert data["collisions"][0]["lat"] == 45.5017
    
    def test_collision_heatmap_with_severity_filter(self, client):
        """Tester l'endpoint heatmap avec un filtre de gravité."""
        payload = {
            "time_range": "2023-01-01 to 2023-01-31",
            "severity_filter": 4
        }
        
        with patch('routes.collision_heatmap.CollisionHeatMapQuery') as mock_query_class:
            mock_query = MagicMock()
            mock_query_class.return_value = mock_query
            mock_query.execute.return_value = {
                "collisions": [],
                "total_count": 0
            }
            
            response = client.post("/dashboard/collision-heatmap", json=payload)
            
            assert response.status_code == 200
            mock_query.execute.assert_called_once_with(
                time_range="2023-01-01 to 2023-01-31",
                severity_filter=4,
                death_nb=None,
                severely_injured_nb=None,
                lightly_injured_nb=None
            )
    
    def test_collision_heatmap_with_injury_filters(self, client):
        """Tester l'endpoint heatmap avec des filtres de blessés."""
        payload = {
            "time_range": "last_week",
            "death_nb": 1,
            "severely_injured_nb": 2,
            "lightly_injured_nb": 5
        }
        
        with patch('routes.collision_heatmap.CollisionHeatMapQuery') as mock_query_class:
            mock_query = MagicMock()
            mock_query_class.return_value = mock_query
            mock_query.execute.return_value = {
                "collisions": [],
                "total_count": 0
            }
            
            response = client.post("/dashboard/collision-heatmap", json=payload)
            
            assert response.status_code == 200
            mock_query.execute.assert_called_once_with(
                time_range="last_week",
                severity_filter=None,
                death_nb=1,
                severely_injured_nb=2,
                lightly_injured_nb=5
            )


class TestWeatherCorrelationEndpoint:
    """Tests pour l'endpoint /dashboard/weather-correlation."""
    
    def test_weather_correlation_endpoint_success(self, client):
        """Tester l'endpoint weather correlation avec des résultats valides."""
        payload = {
            "start_date": "2021-01-01",
            "end_date": "2021-01-31",
            "frequency": "week"
        }
        
        with patch('routes.weather_correlation.WeatherCorrelationQuery') as mock_query_class:
            mock_query = MagicMock()
            mock_query_class.return_value = mock_query
            mock_query.execute.return_value = {
                "summary": {
                    "start_date": "2021-01-01",
                    "end_date": "2021-01-31",
                    "frequency": "week",
                    "total_periods": 4,
                    "total_collisions": 400,
                    "avg_collisions_per_period": 100.0
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
                            "by_severity": {"Léger": 45, "Grave": 10}
                        }
                    }
                ],
                "temperature_analysis": {
                    "cold": {"threshold": "< -10°C", "periods": 2, "avg_collisions": 102.5}
                },
                "precipitation_analysis": {},
                "snow_analysis": {},
                "top_periods": []
            }
            
            response = client.post("/dashboard/weather-correlation", json=payload)
            
            assert response.status_code == 200
            data = response.json()
            assert data["summary"]["total_periods"] == 4
            assert data["summary"]["total_collisions"] == 400
            assert len(data["correlations"]) == 1
    
    def test_weather_correlation_monthly_frequency(self, client):
        """Tester l'endpoint weather correlation avec fréquence mensuelle."""
        payload = {
            "start_date": "2021-01-01",
            "end_date": "2021-12-31",
            "frequency": "month"
        }
        
        with patch('routes.weather_correlation.WeatherCorrelationQuery') as mock_query_class:
            mock_query = MagicMock()
            mock_query_class.return_value = mock_query
            mock_query.execute.return_value = {
                "summary": {
                    "total_periods": 12,
                    "total_collisions": 1200,
                    "avg_collisions_per_period": 100.0
                },
                "correlations": [],
                "temperature_analysis": {},
                "precipitation_analysis": {},
                "snow_analysis": {},
                "top_periods": []
            }
            
            response = client.post("/dashboard/weather-correlation", json=payload)
            
            assert response.status_code == 200
            mock_query.execute.assert_called_once_with(
                start_date="2021-01-01",
                end_date="2021-12-31",
                frequency="month"
            )
    
    def test_weather_correlation_endpoint_error(self, client):
        """Tester l'endpoint weather correlation avec une erreur API."""
        payload = {
            "start_date": "2021-01-01",
            "end_date": "2021-01-31",
            "frequency": "week"
        }
        
        with patch('routes.weather_correlation.WeatherCorrelationQuery') as mock_query_class:
            mock_query = MagicMock()
            mock_query_class.return_value = mock_query
            mock_query.execute.return_value = {
                "error": "Weather API not available"
            }
            
            response = client.post("/dashboard/weather-correlation", json=payload)
            
            assert response.status_code == 400
            data = response.json()
            assert "Weather API not available" in data["detail"]
    
    def test_weather_correlation_endpoint_exception(self, client):
        """Tester l'endpoint weather correlation avec une exception."""
        payload = {
            "start_date": "2021-01-01",
            "end_date": "2021-01-31"
        }
        
        with patch('routes.weather_correlation.WeatherCorrelationQuery') as mock_query_class:
            mock_query = MagicMock()
            mock_query_class.return_value = mock_query
            mock_query.execute.side_effect = Exception("Database error")
            
            response = client.post("/dashboard/weather-correlation", json=payload)
            
            assert response.status_code == 500


class TestTrendsEndpoint:
    """Tests pour l'endpoint /dashboard/trends."""

    def test_trends_endpoint_success(self, client):
        payload = {
            "as_of_date": "2024-04-28"
        }

        mock_result = {
            "generated_at": "2026-02-23T17:45:00Z",
            "as_of_date": "2024-04-28",
            "monthly_collisions": {
                "current_period": "2024-04",
                "previous_period": "2024-03",
                "current_count": 120,
                "previous_count": 110,
                "diff": 10,
                "pct_change": 9.1,
                "direction": "up",
                "series": []
            },
            "pedestrian_3m_vs_last_year": {
                "direction": "up",
                "pct_change": 18.0
            },
            "hourly_peak_shift": {
                "shift_hours": -2,
                "direction": "down"
            },
            "weekly_311_changes": {
                "changes": []
            },
            "weak_signals_311": {
                "signals": []
            },
            "insights": [
                "Collisions en hausse sur le dernier mois."
            ]
        }

        with patch('routes.trends.TrendQuery') as mock_query_class:
            mock_query = MagicMock()
            mock_query_class.return_value = mock_query
            mock_query.execute.return_value = mock_result

            response = client.post("/dashboard/trends", json=payload)

            assert response.status_code == 200
            data = response.json()
            assert data["as_of_date"] == "2024-04-28"
            assert data["monthly_collisions"]["direction"] == "up"
            mock_query.execute.assert_called_once_with(as_of_date="2024-04-28")

    def test_trends_endpoint_value_error(self, client):
        payload = {
            "as_of_date": "2024-99-99"
        }

        with patch('routes.trends.TrendQuery') as mock_query_class:
            mock_query = MagicMock()
            mock_query_class.return_value = mock_query
            mock_query.execute.side_effect = ValueError("Invalid as_of_date format")

            response = client.post("/dashboard/trends", json=payload)

            assert response.status_code == 400
            data = response.json()
            assert "Invalid as_of_date format" in data["detail"]

    def test_trends_endpoint_exception(self, client):
        payload = {}

        with patch('routes.trends.TrendQuery') as mock_query_class:
            mock_query = MagicMock()
            mock_query_class.return_value = mock_query
            mock_query.execute.side_effect = Exception("Unexpected DB issue")

            response = client.post("/dashboard/trends", json=payload)

            assert response.status_code == 500
            data = response.json()
            assert "Unexpected DB issue" in data["detail"]


class TestCollisionForecastJ1Endpoint:
    """Tests pour l'endpoint /dashboard/collision-forecast-j1."""

    def test_collision_forecast_j1_success(self, client):
        payload = {"as_of_date": "2021-12-30"}
        mock_result = {
            "as_of_date": "2021-12-30",
            "forecast_date": "2021-12-31",
            "nb_leger": 14,
            "nb_grave": 1,
            "nb_mortel": 0,
            "model_version": "collision_j1_v1_tuned",
            "selected_models": {
                "leger": "baseline_rolling7",
                "grave": "baseline_rolling7",
                "mortel": "absolute_base",
            },
            "raw_predictions": {
                "leger": 14.212,
                "grave": 0.721,
                "mortel": 0.102,
            },
        }

        with patch("routes.collision_forecast.CollisionForecastService") as mock_service_class:
            mock_service = MagicMock()
            mock_service_class.return_value = mock_service
            mock_service.predict_j1.return_value = mock_result

            response = client.post("/dashboard/collision-forecast-j1", json=payload)

            assert response.status_code == 200
            data = response.json()
            assert data["forecast_date"] == "2021-12-31"
            assert data["nb_leger"] == 14
            mock_service.predict_j1.assert_called_once_with(as_of_date="2021-12-30")

    def test_collision_forecast_j1_not_found(self, client):
        payload = {"model_dir": "data/models/not_found"}

        with patch("routes.collision_forecast.CollisionForecastService") as mock_service_class:
            mock_service_class.side_effect = FileNotFoundError("Model directory not found")

            response = client.post("/dashboard/collision-forecast-j1", json=payload)

            assert response.status_code == 404
            data = response.json()
            assert "Model directory not found" in data["detail"]

    def test_collision_forecast_j1_bad_request(self, client):
        payload = {"as_of_date": "2029-01-01"}

        with patch("routes.collision_forecast.CollisionForecastService") as mock_service_class:
            mock_service = MagicMock()
            mock_service_class.return_value = mock_service
            mock_service.predict_j1.side_effect = ValueError("as_of_date is after max available date")

            response = client.post("/dashboard/collision-forecast-j1", json=payload)

            assert response.status_code == 400
            data = response.json()
            assert "after max available date" in data["detail"]
