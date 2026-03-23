from __future__ import annotations
import logging
import re
from typing import Any
import pandas as pd
from playwright.async_api import async_playwright, Page, Locator
from scrapers.base import BaseTariffScraper
#logger
logger = logging.getLogger(__name__)

#implementation of BaseTariffScraper
class BordGaisScraper(BaseTariffScraper):
    supplier_name = "Bord Gais Energy"

    BASE_URL = "https://www.bordgaisenergy.ie/home/our-plans"
    SMART_PARAMS = "?isNewCustomer=YES&fuelType=ELECTRICITY&smartMeter=SMARTMETER_YES&isSmartMeter=true"
    STANDARD_PARAMS = "?isNewCustomer=YES&fuelType=ELECTRICITY&smartMeter=SMARTMETER_NO"

    #target url that is in monitor script
    target_url = BASE_URL + SMART_PARAMS

    CARD_SELECTOR = "div.plan-card--elec"
    #scrape plans smart and non smart
    async def scrape(self) -> pd.DataFrame:
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True)
            context = await browser.new_context(
                viewport={"width": 1280, "height": 900},
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
            )
            page = await context.new_page()

            try:
                rows: list[dict[str, Any]] = []

                #smart meter plans
                smart_url = self.BASE_URL + self.SMART_PARAMS
                logger.info("Navigating to smart plans: %s", smart_url)
                await page.goto(smart_url, wait_until="domcontentloaded", timeout=60_000)
                await page.wait_for_timeout(3_000)  # let JS render cards
                await self._dismiss_banners(page)

                smart_rows = await self._extract_listing(page, meter_type="smart")
                logger.info("Found %d smart meter plans", len(smart_rows))
                rows.extend(smart_rows)

                #non smart meter plans
                standard_url = self.BASE_URL + self.STANDARD_PARAMS
                logger.info("Navigating to standard plans: %s", standard_url)
                await page.goto(standard_url, wait_until="domcontentloaded", timeout=60_000)
                await page.wait_for_timeout(3_000)

                standard_rows = await self._extract_listing(page, meter_type="24h")
                logger.info("Found %d standard meter plans", len(standard_rows))
                rows.extend(standard_rows)

                #fetch standing charges
                for row in rows:
                    detail_url = row.pop("_detail_url", None)
                    if detail_url:
                        try:
                            standing = await self._get_standing_charge(page, detail_url)
                            row["standing_charge_eur_year"] = standing
                        except Exception:
                            logger.warning(
                                "Could not get standing charge for %s",
                                row.get("plan_name"),
                            )

                logger.info("Extracted %d total plans", len(rows))
                return pd.DataFrame(rows)

            finally:
                await browser.close()

    #extract all plans from listingpage
    async def _extract_listing(
        self, page: Page, meter_type: str
    ) -> list[dict[str, Any]]:
        cards = page.locator(self.CARD_SELECTOR)
        count = await cards.count()
        logger.info("Found %d plan cards on listing", count)

        rows: list[dict[str, Any]] = []
        for i in range(count):
            card = cards.nth(i)
            try:
                row = await self._extract_card(card, meter_type)
                if row.get("plan_name"):
                    rows.append(row)
            except Exception:
                logger.exception("Failed to extract card %d", i)
        return rows
    #extract from single card
    async def _extract_card(
        self, card: Locator, meter_type: str
    ) -> dict[str, Any]:
        row: dict[str, Any] = {"meter_type": meter_type}
        text = await card.inner_text()

        #plan name
        name_match = re.search(
            r"(Smart.*?Discount|Smart.*?Electricity|Standard Variable.*?Electricity"
            r"|Electricity\s+Discount)",
            text, re.I | re.S,
        )
        row["plan_name"] = name_match.group(0).strip() if name_match else None

        #discount
        disc_match = re.search(r"(\d+)\s*%\s*Discount", text, re.I)
        row["discount_percent"] = disc_match.group(1) if disc_match else "0"

        #rates
        rates = dict(re.findall(
            r"(Day|Night|Peak|Electricity)\s+(\d+\.\d+)", text, re.I
        ))

        if "Electricity" in rates:
            #single rate plan
            row["unit_rate_day_eur_kwh"] = rates["Electricity"]
            row["unit_rate_24h_eur_kwh"] = rates["Electricity"]
        else:
            row["unit_rate_day_eur_kwh"] = rates.get("Day")
            row["unit_rate_night_eur_kwh"] = rates.get("Night")
            row["unit_rate_peak_eur_kwh"] = rates.get("Peak")

        #structure
        has_peak = row.get("unit_rate_peak_eur_kwh") is not None
        has_night = row.get("unit_rate_night_eur_kwh") is not None
        if has_peak:
            row["structure"] = "3-Period"
        elif has_night:
            row["structure"] = "DayNight"
        else:
            row["structure"] = "24h"

        #standing charge page url
        try:
            link = card.locator("a").filter(has_text="View plan").first
            href = await link.get_attribute("href")
            if href:
                if href.startswith("/"):
                    href = "https://www.bordgaisenergy.ie" + href
                row["_detail_url"] = href
        except Exception:
            pass

        #reference
        eab_match = re.search(r"[\u20ac]([\d,]+)", text)
        if eab_match:
            row["_eab"] = eab_match.group(1).replace(",", "")

        #special rules
        plan_name = (row.get("plan_name") or "").lower()
        if "free saturday" in plan_name:
            row["special_rules"] = '[{"type": "free_hours", "start": 0, "end": 24, "days": ["Saturday"]}]'
        elif "free sunday" in plan_name:
            row["special_rules"] = '[{"type": "free_hours", "start": 0, "end": 24, "days": ["Sunday"]}]'
        elif "weekend" in plan_name and "weekender" not in plan_name:
            row["special_rules"] = '[{"type": "free_weekend"}]'

        return row

    #detail page and extract standing charge
    async def _get_standing_charge(self, page: Page, url: str) -> str | None:
        logger.debug("Fetching standing charge from %s", url)
        await page.goto(url, wait_until="domcontentloaded", timeout=30_000)
        await page.wait_for_timeout(2_000)

        text = await page.locator("body").inner_text()

        #look for standing charge
        m = re.search(
            r"Annual\s+Standing\s+Charge\s*[\n\r]*\s*\u20ac([\d,.]+)",
            text, re.I | re.S,
        )
        if m:
            standing = m.group(1).replace(",", "")
            #pso levy add
            pso = re.search(
                r"PSO\s+levy\s+of\s+\u20ac([\d,.]+)", text, re.I
            )
            if pso:
                pso_val = float(pso.group(1).replace(",", ""))
                standing = str(round(float(standing) + pso_val, 2))
            return standing
        return None

    #helpers, click cookies away
    async def _dismiss_banners(self, page: Page) -> None:
        for selector in [
            "button:has-text('Accept All')",
            "button:has-text('Accept')",
            '#onetrust-accept-btn-handler',
            '[class*="cookie"] button',
        ]:
            try:
                btn = page.locator(selector).first
                if await btn.is_visible(timeout=3_000):
                    await btn.click()
                    await page.wait_for_timeout(500)
                    logger.debug("Dismissed banner via %s", selector)
                    return
            except Exception:
                continue