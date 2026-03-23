from __future__ import annotations
import logging
from typing import Any
import pandas as pd
from playwright.async_api import async_playwright, Locator
from scrapers.base import BaseTariffScraper
#logger
logger = logging.getLogger(__name__)


class TemplateScraper(BaseTariffScraper):
    supplier_name = "Template Supplier"
    target_url = "https://www.example.ie/tariffs"

    CARD_SELECTOR = "div.plan-card"

    async def scrape(self) -> pd.DataFrame:
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True)
            context = await browser.new_context(
                viewport={"width": 1280, "height": 900},
            )
            page = await context.new_page()

            try:
                await page.goto(
                    self.target_url,
                    wait_until="networkidle",
                    timeout=60_000,
                )

                cards = page.locator(self.CARD_SELECTOR)
                count = await cards.count()
                logger.info("Found %d cards", count)

                rows: list[dict[str, Any]] = []
                for i in range(count):
                    card = cards.nth(i)
                    row = await self._extract_card(card)
                    if row.get("plan_name"):
                        rows.append(row)

                return pd.DataFrame(rows)

            finally:
                await browser.close()

    async def _extract_card(self, card: Locator) -> dict[str, Any]:
        row: dict[str, Any] = {}

        try:
            heading = card.locator("h2, h3").first
            row["plan_name"] = (await heading.inner_text()).strip()
        except Exception:
            row["plan_name"] = None

        row["meter_type"] = await card.get_attribute("data-meter-type")

        # TODO: extract rates per this supplier layout
        return row