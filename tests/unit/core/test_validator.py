import pytest


class FakeHumanMessage:
    def __init__(self, content: str):
        self.content = content
        self.type = "human"

@pytest.fixture
def validator_module(core_node_importer):
    return core_node_importer(
        "core.nodes.validator",
        message_symbols={"HumanMessage": FakeHumanMessage},
        extra_modules={"langchain_community.utilities": {"SQLDatabase": object}},
    )


def test_execute_sql_node_returns_error_when_query_missing(validator_module) -> None:
    state = {"generated_query": None}

    result = validator_module.execute_sql_node(state)

    assert result["query_error"] == "Requête vide ou non générée."


def test_execute_sql_node_runs_query_successfully(
    monkeypatch: pytest.MonkeyPatch, validator_module
) -> None:
    called = {}

    class FakeDB:
        def run(self, query: str):
            called["query"] = query
            return [("ok", 1)]

    class FakeSQLDatabase:
        @staticmethod
        def from_uri(uri: str):
            called["uri"] = uri
            return FakeDB()

    monkeypatch.setattr(validator_module, "SQLDatabase", FakeSQLDatabase)

    result = validator_module.execute_sql_node({"generated_query": "SELECT 1"})

    assert result["query_results"] == "[('ok', 1)]"
    assert result["query_error"] is None
    assert called["query"] == "SELECT 1"
    assert called["uri"].startswith("sqlite:///")
    assert called["uri"].endswith("/data/db/mobility.db")


def test_execute_sql_node_returns_feedback_message_on_exception(
    monkeypatch: pytest.MonkeyPatch, validator_module
) -> None:
    class FakeSQLDatabase:
        @staticmethod
        def from_uri(_uri: str):
            raise RuntimeError("database unavailable")

    monkeypatch.setattr(validator_module, "SQLDatabase", FakeSQLDatabase)

    result = validator_module.execute_sql_node({"generated_query": "SELECT * FROM trips"})

    assert result["query_error"] == "database unavailable"
    assert isinstance(result["messages"], FakeHumanMessage)
    assert "SQL query failed" in result["messages"].content
