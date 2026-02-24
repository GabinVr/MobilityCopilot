import logging

from data.Requetes311SyncService import (
	HTTPFileDownloader,
	Requetes311Scraper,
	Requetes311SyncService,
	RequestsPageFetcher,
)
from data.ingest import Requetes311Store

DATASET_URL = "https://donnees.montreal.ca/dataset/requete-311"

logger = logging.getLogger(__name__)


def update_311_requests() -> int:
	scraper = Requetes311Scraper(fetcher=RequestsPageFetcher())
	downloader = HTTPFileDownloader()
	store = Requetes311Store()
	service = Requetes311SyncService(
		scraper=scraper,
		downloader=downloader,
		store=store,
	)

	logger.info("Running scheduled requetes311 update")
	return service.sync(DATASET_URL)

if __name__ == "__main__":
    update_311_requests()