from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib import error, request

DEFAULT_311_URL = (
    "https://donnees.montreal.ca/dataset/"
    "5866f832-676d-4b07-be6a-e99c21eb17e4/resource/"
    "2cfa0e06-9be4-49a6-b7f1-ee9f2363a872/download/requetes311.csv"
)
DEFAULT_TARGET_FILE = "data/csv/requetes311.csv"
DEFAULT_STATE_FILE = "data/db/requetes311_monitor_state.json"

LOGGER = logging.getLogger("requetes311.monitor")


@dataclass
class RemoteFetchResult:
    not_modified: bool
    tmp_path: Path | None = None
    sha256: str | None = None
    size_bytes: int = 0
    headers: dict[str, str] | None = None


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def sha256_file(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest()


class Requetes311Monitor:
    def __init__(
        self,
        url: str,
        target_file: str | Path,
        state_file: str | Path,
        timeout_seconds: int = 120,
    ) -> None:
        self.url = url
        self.target_file = Path(target_file)
        self.state_file = Path(state_file)
        self.timeout_seconds = timeout_seconds

    def load_state(self) -> dict[str, Any]:
        if not self.state_file.exists():
            return {}
        try:
            return json.loads(self.state_file.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            LOGGER.warning("State file is not valid JSON: %s", self.state_file)
            return {}

    def save_state(self, state: dict[str, Any]) -> None:
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        self.state_file.write_text(
            json.dumps(state, indent=2, sort_keys=True),
            encoding="utf-8",
        )

    def _build_conditional_headers(
        self, state: dict[str, Any], has_local_file: bool
    ) -> dict[str, str]:
        if not has_local_file:
            return {}

        etag = state.get("etag")
        if isinstance(etag, str) and etag:
            return {"If-None-Match": etag}

        last_modified = state.get("last_modified")
        if isinstance(last_modified, str) and last_modified:
            return {"If-Modified-Since": last_modified}

        return {}

    def _stream_response_to_tempfile(
        self, response: Any
    ) -> tuple[Path, str, int]:
        self.target_file.parent.mkdir(parents=True, exist_ok=True)

        hasher = hashlib.sha256()
        size_bytes = 0

        with tempfile.NamedTemporaryFile(
            mode="wb",
            prefix="requetes311_",
            suffix=".tmp",
            dir=self.target_file.parent,
            delete=False,
        ) as temp_handle:
            while True:
                chunk = response.read(1024 * 1024)
                if not chunk:
                    break
                temp_handle.write(chunk)
                hasher.update(chunk)
                size_bytes += len(chunk)

        return Path(temp_handle.name), hasher.hexdigest(), size_bytes

    def _fetch_remote_file(
        self, conditional_headers: dict[str, str]
    ) -> RemoteFetchResult:
        req = request.Request(
            self.url,
            headers=conditional_headers,
            method="GET",
        )
        try:
            with request.urlopen(req, timeout=self.timeout_seconds) as response:
                headers = {key: value for key, value in response.headers.items()}
                tmp_path, remote_sha256, size_bytes = self._stream_response_to_tempfile(
                    response
                )
                return RemoteFetchResult(
                    not_modified=False,
                    tmp_path=tmp_path,
                    sha256=remote_sha256,
                    size_bytes=size_bytes,
                    headers=headers,
                )
        except error.HTTPError as exc:
            if exc.code == 304:
                headers = {key: value for key, value in exc.headers.items()}
                return RemoteFetchResult(
                    not_modified=True,
                    headers=headers,
                )
            raise

    @staticmethod
    def _merge_headers(state: dict[str, Any], headers: dict[str, str]) -> None:
        header_mapping = {
            "etag": "ETag",
            "last_modified": "Last-Modified",
            "content_length": "Content-Length",
        }
        for state_key, header_key in header_mapping.items():
            value = headers.get(header_key)
            if value:
                state[state_key] = value

    def check_once(self) -> dict[str, Any]:
        state = self.load_state()
        state.setdefault("checks_count", 0)
        state.setdefault("changes_count", 0)
        state["url"] = self.url
        state["last_check_at"] = utc_now_iso()
        state["checks_count"] += 1

        had_local_file = self.target_file.exists()

        previous_hash = state.get("sha256")
        if not isinstance(previous_hash, str):
            previous_hash = None

        if had_local_file and previous_hash is None:
            previous_hash = sha256_file(self.target_file)
            state["sha256"] = previous_hash

        conditional_headers = self._build_conditional_headers(state, had_local_file)

        try:
            fetch_result = self._fetch_remote_file(conditional_headers)
        except Exception as exc:
            state["last_result"] = "error"
            state["last_error"] = str(exc)
            self.save_state(state)
            raise

        headers = fetch_result.headers or {}
        self._merge_headers(state, headers)

        if fetch_result.not_modified:
            state["last_result"] = "unchanged"
            state["last_error"] = None
            self.save_state(state)
            return {"changed": False, "reason": "not_modified"}

        if fetch_result.tmp_path is None or fetch_result.sha256 is None:
            state["last_result"] = "error"
            state["last_error"] = "Download completed without payload."
            self.save_state(state)
            raise RuntimeError("Download completed without payload.")

        new_hash = fetch_result.sha256
        temp_path = fetch_result.tmp_path
        changed = True
        reason = "hash_changed"

        if had_local_file and previous_hash == new_hash:
            changed = False
            reason = "same_hash"
            temp_path.unlink(missing_ok=True)
        else:
            self.target_file.parent.mkdir(parents=True, exist_ok=True)
            temp_path.replace(self.target_file)
            if not had_local_file:
                reason = "initial_download"

        state["sha256"] = new_hash
        state["bytes"] = fetch_result.size_bytes
        state["last_result"] = "changed" if changed else "unchanged"
        state["last_error"] = None

        if changed:
            state["changes_count"] += 1
            state["last_change_at"] = utc_now_iso()

        self.save_state(state)

        return {
            "changed": changed,
            "reason": reason,
            "previous_hash": previous_hash,
            "current_hash": new_hash,
            "size_bytes": fetch_result.size_bytes,
        }

    def run(self, interval_seconds: float, run_once: bool = False) -> None:
        while True:
            started_at = time.monotonic()
            try:
                result = self.check_once()
                if result["changed"]:
                    LOGGER.info(
                        "Change detected (%s). size=%s bytes",
                        result["reason"],
                        result.get("size_bytes"),
                    )
                else:
                    LOGGER.info("No change detected (%s).", result["reason"])
            except Exception:
                LOGGER.exception("Check failed.")

            if run_once:
                return

            elapsed = time.monotonic() - started_at
            sleep_seconds = max(0.0, interval_seconds - elapsed)
            time.sleep(sleep_seconds)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Monitor Montreal 311 CSV and detect remote changes "
            "against local file."
        )
    )
    parser.add_argument(
        "--url",
        default=os.getenv("REQUETES311_URL", DEFAULT_311_URL),
        help="CSV URL to monitor.",
    )
    parser.add_argument(
        "--target-file",
        default=os.getenv("REQUETES311_TARGET_FILE", DEFAULT_TARGET_FILE),
        help="Local CSV file path.",
    )
    parser.add_argument(
        "--state-file",
        default=os.getenv("REQUETES311_STATE_FILE", DEFAULT_STATE_FILE),
        help="JSON state file path.",
    )
    parser.add_argument(
        "--checks-per-day",
        type=int,
        default=int(os.getenv("REQUETES311_CHECKS_PER_DAY", "10")),
        help="Number of checks per day.",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=int(os.getenv("REQUETES311_TIMEOUT_SECONDS", "120")),
        help="HTTP timeout per request in seconds.",
    )
    parser.add_argument(
        "--log-level",
        default=os.getenv("REQUETES311_LOG_LEVEL", "INFO"),
        help="Python logging level.",
    )
    parser.add_argument(
        "--run-once",
        action="store_true",
        help="Run a single check then exit.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.checks_per_day <= 0:
        raise ValueError("--checks-per-day must be > 0")

    log_level = getattr(logging, str(args.log_level).upper(), logging.INFO)
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    interval_seconds = 86400 / args.checks_per_day
    LOGGER.info(
        "Starting 311 monitor (checks_per_day=%s, interval_seconds=%.1f).",
        args.checks_per_day,
        interval_seconds,
    )

    monitor = Requetes311Monitor(
        url=args.url,
        target_file=args.target_file,
        state_file=args.state_file,
        timeout_seconds=args.timeout_seconds,
    )
    monitor.run(interval_seconds=interval_seconds, run_once=args.run_once)


if __name__ == "__main__":
    main()
