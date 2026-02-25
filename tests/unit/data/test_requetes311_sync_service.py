import sqlite3

from data.Requetes311SyncService import (
    HTTPFileDownloader,
    PageFetcher,
    Requetes311Scraper,
    Requetes311SyncService,
)
from data.ingest import Requetes311Store


class FakePageFetcher(PageFetcher):
    def __init__(self, html: str):
        self.html = html

    def fetch(self, url: str) -> str:
        return self.html


class FakeDownloader(HTTPFileDownloader):
    def __init__(self, content: str):
        self.content = content

    def download(self, url: str, dest_path: str) -> None:
        with open(dest_path, "w", encoding="utf-8") as handle:
            handle.write(self.content)


def _build_html(items):
    blocks = []
    for title, href in items:
        blocks.append(
            "<div class=\"list-group-item list-group-item-action border-0 pl-0 group\" role=\"listitem\">"
            "<div class=\"row\">"
            f"<div class=\"list-group-item-title\">{title}</div>"
            f"<a title=\"Télécharger\" href=\"{href}\"></a>"
            "</div>"
            "</div>"
        )
    return "<div id=\"resources\">" + "".join(blocks) + "</div>"


def test_parser_selects_latest_open_ended_resource():
    html = _build_html(
        [
            ("Requêtes 3-1-1 (archives 2019 à 2021)", "https://example.com/archive.csv"),
            ("Requêtes 3-1-1 (2022 à ce jour)", "https://example.com/latest.csv"),
        ]
    )
    scraper = Requetes311Scraper(fetcher=FakePageFetcher(html))

    url = scraper.find_latest_csv_url("https://example.com")

    assert url == "https://example.com/latest.csv"


def test_sync_adds_only_new_rows(tmp_path):
    html = _build_html(
        [("Requêtes 3-1-1 (2022 à ce jour)", "https://example.com/latest.csv")]
    )
    csv_content = "ID_UNIQUE,DDS_DATE_CREATION\n1,2024-01-01\n2,2024-01-02\n"

    db_path = tmp_path / "mobility.db"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE requetes311 (ID_UNIQUE TEXT, DDS_DATE_CREATION TEXT)")
    conn.execute("INSERT INTO requetes311 (ID_UNIQUE, DDS_DATE_CREATION) VALUES (?, ?)", ("1", "2024-01-01"))
    conn.commit()
    conn.close()

    scraper = Requetes311Scraper(fetcher=FakePageFetcher(html))
    downloader = FakeDownloader(csv_content)
    store = Requetes311Store(db_path=str(db_path))
    service = Requetes311SyncService(
        scraper=scraper,
        downloader=downloader,
        store=store,
    )

    added = service.sync("https://example.com")

    assert added == 1
    conn = sqlite3.connect(db_path)
    rows = conn.execute("SELECT COUNT(*) FROM requetes311").fetchone()[0]
    conn.close()
    assert rows == 2
