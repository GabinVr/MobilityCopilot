import hashlib
import json
from pathlib import Path

from data.requetes311_monitor import RemoteFetchResult, Requetes311Monitor


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _build_monitor(tmp_path: Path) -> Requetes311Monitor:
    return Requetes311Monitor(
        url="https://example.com/requetes311.csv",
        target_file=tmp_path / "csv" / "requetes311.csv",
        state_file=tmp_path / "db" / "requetes311_monitor_state.json",
        timeout_seconds=5,
    )


def test_initial_download_creates_file_and_state(tmp_path, monkeypatch):
    monitor = _build_monitor(tmp_path)
    payload = b"id,valeur\n1,test\n"
    remote_tmp = tmp_path / "remote_payload.tmp"
    remote_tmp.write_bytes(payload)

    def fake_fetch(headers):
        assert headers == {}
        return RemoteFetchResult(
            not_modified=False,
            tmp_path=remote_tmp,
            sha256=_sha256(payload),
            size_bytes=len(payload),
            headers={
                "ETag": '"v1"',
                "Last-Modified": "Mon, 01 Jan 2024 00:00:00 GMT",
                "Content-Length": str(len(payload)),
            },
        )

    monkeypatch.setattr(monitor, "_fetch_remote_file", fake_fetch)
    result = monitor.check_once()

    assert result["changed"] is True
    assert result["reason"] == "initial_download"
    assert monitor.target_file.read_bytes() == payload

    state = json.loads(monitor.state_file.read_text(encoding="utf-8"))
    assert state["sha256"] == _sha256(payload)
    assert state["etag"] == '"v1"'
    assert state["checks_count"] == 1
    assert state["changes_count"] == 1
    assert state["last_result"] == "changed"


def test_304_not_modified_uses_etag_condition(tmp_path, monkeypatch):
    monitor = _build_monitor(tmp_path)
    payload = b"id,valeur\n1,test\n"

    monitor.target_file.parent.mkdir(parents=True, exist_ok=True)
    monitor.target_file.write_bytes(payload)

    monitor.save_state(
        {
            "sha256": _sha256(payload),
            "etag": '"v1"',
            "checks_count": 1,
            "changes_count": 0,
        }
    )

    captured = {}

    def fake_fetch(headers):
        captured["headers"] = headers
        return RemoteFetchResult(not_modified=True, headers={})

    monkeypatch.setattr(monitor, "_fetch_remote_file", fake_fetch)
    result = monitor.check_once()

    assert captured["headers"] == {"If-None-Match": '"v1"'}
    assert result["changed"] is False
    assert result["reason"] == "not_modified"

    state = json.loads(monitor.state_file.read_text(encoding="utf-8"))
    assert state["checks_count"] == 2
    assert state["changes_count"] == 0
    assert state["last_result"] == "unchanged"


def test_same_hash_download_does_not_replace_file(tmp_path, monkeypatch):
    monitor = _build_monitor(tmp_path)
    payload = b"id,valeur\n1,test\n"

    monitor.target_file.parent.mkdir(parents=True, exist_ok=True)
    monitor.target_file.write_bytes(payload)
    monitor.save_state(
        {
            "sha256": _sha256(payload),
            "checks_count": 0,
            "changes_count": 0,
        }
    )

    remote_tmp = tmp_path / "remote_same.tmp"
    remote_tmp.write_bytes(payload)

    def fake_fetch(headers):
        assert headers == {}
        return RemoteFetchResult(
            not_modified=False,
            tmp_path=remote_tmp,
            sha256=_sha256(payload),
            size_bytes=len(payload),
            headers={"Content-Length": str(len(payload))},
        )

    monkeypatch.setattr(monitor, "_fetch_remote_file", fake_fetch)
    result = monitor.check_once()

    assert result["changed"] is False
    assert result["reason"] == "same_hash"
    assert monitor.target_file.read_bytes() == payload
    assert not remote_tmp.exists()

    state = json.loads(monitor.state_file.read_text(encoding="utf-8"))
    assert state["changes_count"] == 0
    assert state["last_result"] == "unchanged"


def test_hash_change_replaces_local_file(tmp_path, monkeypatch):
    monitor = _build_monitor(tmp_path)
    old_payload = b"id,valeur\n1,old\n"
    new_payload = b"id,valeur\n1,new\n"

    monitor.target_file.parent.mkdir(parents=True, exist_ok=True)
    monitor.target_file.write_bytes(old_payload)
    monitor.save_state(
        {
            "sha256": _sha256(old_payload),
            "etag": '"v1"',
            "checks_count": 2,
            "changes_count": 1,
        }
    )

    remote_tmp = tmp_path / "remote_new.tmp"
    remote_tmp.write_bytes(new_payload)

    def fake_fetch(headers):
        assert headers == {"If-None-Match": '"v1"'}
        return RemoteFetchResult(
            not_modified=False,
            tmp_path=remote_tmp,
            sha256=_sha256(new_payload),
            size_bytes=len(new_payload),
            headers={"ETag": '"v2"', "Content-Length": str(len(new_payload))},
        )

    monkeypatch.setattr(monitor, "_fetch_remote_file", fake_fetch)
    result = monitor.check_once()

    assert result["changed"] is True
    assert result["reason"] == "hash_changed"
    assert monitor.target_file.read_bytes() == new_payload

    state = json.loads(monitor.state_file.read_text(encoding="utf-8"))
    assert state["etag"] == '"v2"'
    assert state["sha256"] == _sha256(new_payload)
    assert state["checks_count"] == 3
    assert state["changes_count"] == 2
    assert state["last_result"] == "changed"
