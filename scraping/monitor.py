from __future__ import annotations
import argparse
import hashlib
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from playwright.sync_api import sync_playwright
from scrapers.registry import get_scraper, list_suppliers

logger = logging.getLogger(__name__)

#store page hash
HASH_STORE = Path("data/tariffs/.page_hashes.json")

#checking supplier and trigger scraper if change
class TariffMonitor:

    def __init__(self, hash_store: Path = HASH_STORE):
        self.hash_store = hash_store
        self.hash_store.parent.mkdir(parents=True, exist_ok=True)
        self.hashes = self._load_hashes()

    #api
    #get scraper for each supplier
    def check_and_scrape(self, supplier_key: str) -> bool:
        scraper = get_scraper(supplier_key)
        logger.info("Checking %s at %s", supplier_key, scraper.target_url)
        #generate hash
        current_hash = self._get_page_hash(scraper.target_url)
        #compare with stored and skip scrape is no change
        stored = self.hashes.get(supplier_key)
        if stored and stored["hash"] == current_hash:
            logger.info(
                "No change detected for %s (hash=%s...)",
                supplier_key,
                current_hash[:12],
            )
            return False
        #trigger scraper if change
        logger.info(
            "Change detected for %s (old=%s... new=%s...). Scraping...",
            supplier_key,
            (stored["hash"][:12] if stored else "none"),
            current_hash[:12],
        )

        #run scraper and save data
        output_path = scraper.run_and_save()
        logger.info("Scrape complete: %s", output_path)

        #update hash
        self.hashes[supplier_key] = {
            "hash": current_hash,
            "last_checked": datetime.now(timezone.utc).isoformat(),
            "last_scraped": datetime.now(timezone.utc).isoformat(),
        }
        self._save_hashes()
        return True
    #check all suppliers have been scraped
    def check_all(self) -> dict[str, bool]:
        results = {}
        for key in list_suppliers():
            try:
                results[key] = self.check_and_scrape(key)
            except Exception:
                logger.exception("Error checking %s", key)
                results[key] = False
        return results

    #force scrape regardless of whether change
    def force_scrape(self, supplier_key: str) -> Path:
        scraper = get_scraper(supplier_key)
        output_path = scraper.run_and_save()

        #update hash
        current_hash = self._get_page_hash(scraper.target_url)
        self.hashes[supplier_key] = {
            "hash": current_hash,
            "last_checked": datetime.now(timezone.utc).isoformat(),
            "last_scraped": datetime.now(timezone.utc).isoformat(),
        }
        self._save_hashes()
        return output_path

#hashing, launch headless browser
    @staticmethod
    def _get_page_hash(url: str) -> str:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            page = browser.new_page()
            try:
                #open page
                page.goto(url, wait_until="domcontentloaded", timeout=60_000)
                #wait for JS
                page.wait_for_timeout(3_000)
                main = page.locator("main, [role='main'], #main-content, .content")
                if main.count() > 0:
                    text = main.first.inner_text()
                else:
                    text = page.locator("body").inner_text()

                #normalise whitespace
                text = " ".join(text.split())
                return hashlib.sha256(text.encode()).hexdigest()
            finally:
                browser.close()
    #load and save hashes
    def _load_hashes(self) -> dict:
        if self.hash_store.exists():
            return json.loads(self.hash_store.read_text())
        return {}

    def _save_hashes(self) -> None:
        self.hash_store.write_text(json.dumps(self.hashes, indent=2))

#logging
def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    #parse command line options
    parser = argparse.ArgumentParser(
        description="Monitor tariff pages and scrape on change"
    )
    parser.add_argument(
        "--supplier",
        default=None,
        help="Supplier key (default: check all)",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run a single check then exit",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force scrape even if no change detected",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=0,
        help="Polling interval in seconds (0 = run once)",
    )
    args = parser.parse_args()

    monitor = TariffMonitor()
    #decide based on CLI arguments
    def run_check():
        #force scrape
        if args.force:
            key = args.supplier or "electric_ireland"
            monitor.force_scrape(key)
        elif args.supplier:
            monitor.check_and_scrape(args.supplier)
        else:
            results = monitor.check_all()
            for k, scraped in results.items():
                status = "SCRAPED" if scraped else "no change"
                logger.info("  %s: %s", k, status)

    run_check()

    if args.interval > 0 and not args.once:
        logger.info("Polling every %d seconds. Ctrl+C to stop.", args.interval)
        while True:
            time.sleep(args.interval)
            run_check()


if __name__ == "__main__":
    main()