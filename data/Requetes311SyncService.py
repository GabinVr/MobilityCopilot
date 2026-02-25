import logging
import os
import re
import tempfile
import unicodedata
from abc import ABC, abstractmethod
from dataclasses import dataclass
from html.parser import HTMLParser
from typing import Iterable, List, Optional, Tuple

import pandas as pd
import requests

from data.ingest import DataLoaderFactory, Requetes311Store, SoftDataCleaner


class RemoteFileDownloader(ABC):
    @abstractmethod
    def download(self, url: str, dest_path: str) -> None:
        pass

class HTTPFileDownloader(RemoteFileDownloader):
    def download(self, url: str, dest_path: str) -> None:
        logger.info("Starting 311 CSV download", extra={"url": url})
        response = requests.get(
            url,
            stream=True,
            timeout=(10, 30),
            headers={"User-Agent": "MobilityCopilot/1.0"},
        )
        response.raise_for_status()
        total = response.headers.get("Content-Length")
        total_mb = None
        if total and total.isdigit():
            total_mb = int(total) / (1024 * 1024)
            logger.info("Content length detected", extra={"mb": round(total_mb, 2)})

        bytes_written = 0
        next_log = 50 * 1024 * 1024
        with open(dest_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if not chunk:
                    continue
                f.write(chunk)
                bytes_written += len(chunk)
                if bytes_written >= next_log:
                    logger.info(
                        "Download progress",
                        extra={"mb": round(bytes_written / (1024 * 1024), 2)},
                    )
                    next_log += 50 * 1024 * 1024

        logger.info(
            "Finished 311 CSV download",
            extra={"path": dest_path, "mb": round(bytes_written / (1024 * 1024), 2)},
        )

logger = logging.getLogger(__name__)


class PageFetcher(ABC):
    @abstractmethod
    def fetch(self, url: str) -> str:
        pass


class RequestsPageFetcher(PageFetcher):
    def fetch(self, url: str) -> str:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        return response.text


@dataclass
class ResourceEntry:
    title: str
    download_url: str
class Requetes311Scraper:
    """
    Scraper that scan the dataset page to find the latest CSV file for 311 requests and download it.
    """
    def __init__(self, fetcher: PageFetcher):
        self.fetcher = fetcher

    def find_latest_csv_url(self, dataset_url: str) -> str:
        html = self.fetcher.fetch(dataset_url)
        entries = list(ResourceListParser.parse(html))
        if not entries:
            raise ValueError("No CSV resources found on dataset page")

        best_entry = max(entries, key=self._score_entry)
        logger.info("Selected 311 CSV resource", extra={"title": best_entry.title})
        return best_entry.download_url

    def _score_entry(self, entry: ResourceEntry) -> Tuple[int, int, int, str]:
        normalized = _normalize_text(entry.title)
        years = [int(y) for y in re.findall(r"\b(19\d{2}|20\d{2})\b", normalized)]
        max_year = max(years) if years else 0
        min_year = min(years) if years else 0
        is_open_ended = 1 if ("a ce jour" in normalized or "à ce jour" in normalized) else 0
        return (max_year, is_open_ended, min_year, entry.title)


class Requetes311SyncService:
    def __init__(
        self,
        scraper: Requetes311Scraper,
        downloader: RemoteFileDownloader,
        store: Requetes311Store,
        table_name: str = "requetes311",
    ):
        self.scraper = scraper
        self.downloader = downloader
        self.store = store
        self.table_name = table_name
        self.cleaner = SoftDataCleaner()
        self.loader_factory = DataLoaderFactory()

    def sync(self, dataset_url: str) -> int:
        logger.info("Starting 311 sync")
        csv_url = self.scraper.find_latest_csv_url(dataset_url)

        # Use a unique temporary file to avoid conflicts in concurrent runs
        tmp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".csv", prefix="requetes311_")
        tmp_path = tmp_file.name
        tmp_file.close()  # Close the file handle but keep the file
    
        try:
            logger.info("Downloading 311 CSV", extra={"url": csv_url})
            self.downloader.download(csv_url, tmp_path)

            loader = self.loader_factory.get_loader(tmp_path)
            if loader is None:
                raise ValueError("No loader available for downloaded CSV")

            _, df = loader.load(tmp_path)
            df = self.cleaner.clean(df, self.table_name)

            added = self._append_new_rows(df)
            logger.info("311 sync complete", extra={"added_rows": added})
            return added
        finally:
            try:
                os.remove(tmp_path)
            except OSError:
                logger.warning("Failed to remove temporary CSV", extra={"path": tmp_path})

    def _append_new_rows(self, df: pd.DataFrame) -> int:
        if df.empty:
            logger.info("No rows in downloaded CSV")
            return 0

        added = self.store.append_new_rows(df)
        if added == 0:
            logger.info("No new rows to insert")
        return added


class ResourceListParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self._stack: List[str] = []
        self._current: Optional[ResourceEntry] = None
        self._current_depth: Optional[int] = None
        self._in_title = False
        self._title_buffer: List[str] = []
        self._entries: List[ResourceEntry] = []

    @classmethod
    def parse(cls, html: str) -> Iterable[ResourceEntry]:
        parser = cls()
        parser.feed(html)
        parser.close()
        return parser._entries

    def handle_starttag(self, tag: str, attrs: List[Tuple[str, Optional[str]]]) -> None:
        self._stack.append(tag)
        attr_map = {k: v or "" for k, v in attrs}
        classes = set(attr_map.get("class", "").split())

        if tag == "div" and "list-group-item" in classes and attr_map.get("role") == "listitem":
            self._current = ResourceEntry(title="", download_url="")
            self._current_depth = len(self._stack)

        if tag == "div" and "list-group-item-title" in classes:
            self._in_title = True
            self._title_buffer = []

        if tag == "a":
            title_attr = attr_map.get("title", "")
            if "telecharger" in _normalize_text(title_attr):
                href = attr_map.get("href", "")
                if self._current is not None and href:
                    self._current.download_url = href

    def handle_endtag(self, tag: str) -> None:
        if self._stack:
            self._stack.pop()

        if self._in_title and tag == "div":
            self._in_title = False
            if self._current is not None:
                self._current.title = "".join(self._title_buffer).strip()

        if self._current is not None and self._current_depth is not None:
            if len(self._stack) < self._current_depth:
                if self._current.title and self._current.download_url:
                    if ".csv" in self._current.download_url.lower():
                        self._entries.append(self._current)
                self._current = None
                self._current_depth = None

    def handle_data(self, data: str) -> None:
        if self._in_title:
            self._title_buffer.append(data)


def _normalize_text(text: str) -> str:
    normalized = unicodedata.normalize("NFKD", text)
    ascii_text = normalized.encode("ascii", "ignore").decode("ascii")
    return " ".join(ascii_text.lower().split())

    
    