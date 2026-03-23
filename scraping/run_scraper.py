from __future__ import annotations
import argparse
import logging
import sys
from scrapers.registry import get_scraper, list_suppliers
from monitor import TariffMonitor
#logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

#running scrapers from command line
def main() -> None:
    #command line arguments
    parser = argparse.ArgumentParser(description="Run tariff scrapers")
    parser.add_argument(
        "supplier",
        nargs="?",
        default=None,
        help=f"Supplier key. Available: {', '.join(list_suppliers())}",
    )
    #run all suppliers
    parser.add_argument("--all", action="store_true", help="Scrape all suppliers")
    parser.add_argument("--force", action="store_true", help="Ignore change detection")
    args = parser.parse_args()
    #require supplier
    if not args.supplier and not args.all:
        parser.print_help()
        sys.exit(1)
    #monitor tp handle detection
    monitor = TariffMonitor()

    if args.all:
        suppliers = list_suppliers()
    else:
        suppliers = [args.supplier]
    #loop through suppliers
    for key in suppliers:
        logger.info("=== Processing: %s ===", key)
        try:
            if args.force:
                path = monitor.force_scrape(key)
                logger.info("Force-scraped -> %s", path)
            else:
                changed = monitor.check_and_scrape(key)
                if not changed:
                    logger.info("No change detected, skipping scrape.")
        except Exception:
            logger.exception("Failed to process %s", key)


if __name__ == "__main__":
    main()