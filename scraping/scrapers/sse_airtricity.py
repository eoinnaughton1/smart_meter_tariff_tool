from __future__ import annotations
import logging
import re
from typing import Any
import pandas as pd
from playwright.async_api import async_playwright, Page, Locator
from scrapers.base import BaseTariffScraper
#logger
logger = logging.getLogger(__name__)

#scrapr implementation
class SseAirtricityScraper(BaseTariffScraper):
    supplier_name = "SSE Airtricity"
    target_url = "https://www.sseairtricity.com/ie/home/products/switch-to-sse-airtricity"
    #select card
    CARD_SELECTOR = "div.ProductCardShared"

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
                logger.info("Navigating to %s", self.target_url)
                await page.goto(
                    self.target_url,
                    wait_until="domcontentloaded",
                    timeout=60_000,
                )
                await page.wait_for_timeout(3_000)
                await self._dismiss_banners(page)

                #click electricity filer
                await self._click_filter(page, "Electricity only")

                rows: list[dict[str, Any]] = []

                #standart meter plans
                await self._click_filter(page, "Standard meter")
                std_rows = await self._extract_listing(page, meter_type="24h")
                logger.info("Found %d standard meter plans", len(std_rows))
                rows.extend(std_rows)

                #smart meter plans
                await self._click_filter(page, "Smart meter")
                smart_rows = await self._extract_listing(page, meter_type="smart")
                logger.info("Found %d smart meter plans", len(smart_rows))
                rows.extend(smart_rows)

                #details page for rates and standing charges
                for row in rows:
                    detail_url = row.pop("_detail_url", None)
                    if detail_url:
                        try:
                            await self._extract_detail_page(page, detail_url, row)
                        except Exception:
                            logger.exception(
                                "Could not get details for %s from %s",
                                row.get("plan_name"), detail_url,
                            )

                logger.info("Extracted %d total plans", len(rows))
                return pd.DataFrame(rows)

            finally:
                await browser.close()

    #listing page
    async def _extract_listing(
        self, page: Page, meter_type: str
    ) -> list[dict[str, Any]]:
        cards = page.locator(self.CARD_SELECTOR)
        count = await cards.count()
        logger.info("Found %d ProductCardShared elements", count)

        rows: list[dict[str, Any]] = []
        for i in range(count):
            card = cards.nth(i)
            try:
                if not await card.is_visible(timeout=500):
                    continue
            except Exception:
                continue

            try:
                row = await self._extract_card(card, meter_type)
                if row.get("plan_name"):
                    rows.append(row)
            except Exception:
                logger.exception("Failed to extract card %d", i)
        return rows

    async def _extract_card(
        self, card: Locator, meter_type: str
    ) -> dict[str, Any]:
        row: dict[str, Any] = {"meter_type": meter_type}
        text = await card.inner_text()

        #plan name
        lines = [l.strip() for l in text.splitlines() if l.strip()]
        row["plan_name"] = lines[0] if lines else None

        #Discount
        disc = re.search(r"(\d+)%\s*(?:discount|off)", text, re.I)
        row["discount_percent"] = disc.group(1) if disc else "0"

        #Detail page URL
        try:
            link = card.locator("a").filter(has_text="View plan").first
            href = await link.get_attribute("href")
            if href:
                if href.startswith("/"):
                    href = "https://www.sseairtricity.com" + href
                row["_detail_url"] = href
        except Exception:
            pass

        #speical rules
        name = (row.get("plan_name") or "").lower()
        if "weekend" in name:
            row["special_rules"] = (
                '[{"type": "free_hours", "start": 8, "end": 23, '
                '"days": ["Saturday", "Sunday"], "priority": 1}]'
            )
        elif "ev" in name:
            row["special_rules"] = (
                '[{"type": "ev_night_boost", "start": 23, "end": 5, '
                '"rate": 0.0, "priority": 1}]'
            )

        return row

    #detail page
    async def _extract_detail_page(
        self, page: Page, url: str, row: dict[str, Any]
    ) -> None:
        logger.debug("Fetching detail page: %s", url)
        await page.goto(url, wait_until="domcontentloaded", timeout=30_000)
        await page.wait_for_timeout(3_000)

        tables = page.locator("table")
        table_count = await tables.count()
        logger.debug("Found %d tables on detail page", table_count)

        if table_count == 0:
            logger.warning("No tables found on %s", url)
            return

        meter_type = row.get("meter_type", "")
        found_smart_rates = False

        for t_idx in range(table_count):
            table_text = await tables.nth(t_idx).inner_text()

            #skip eab tables
            if "Estimated annual bill" in table_text:
                continue

            #unit rates table
            if "Discounted rate" in table_text:
                #weekend plan
                if "Weekday Day" in table_text:
                    self._parse_weekend_rates(table_text, row)
                    found_smart_rates = True

                #smart tables
                elif "Smart Day" in table_text or "EV Max" in table_text:
                    if meter_type == "smart":
                        self._parse_unit_rates(table_text, row, meter_type)
                        found_smart_rates = True
                        #clear earlier 24h rate
                        row.pop("unit_rate_24h_eur_kwh", None)

                elif "24hr meter" in table_text:
                    #only use if no smart speicfic found
                    if not found_smart_rates:
                        self._parse_unit_rates(table_text, row, meter_type)

            #standing charges
            elif "per year" in table_text.lower() and "\u20ac" in table_text:
                self._parse_standing_charge(table_text, row, meter_type)

            #levy
            elif "19.10" in table_text or "Cost per year" in table_text:
                pso_match = re.search(r"\u20ac([\d.]+)", table_text)
                if pso_match:
                    row["_pso"] = float(pso_match.group(1))

        #add levy to charge
        standing = row.get("standing_charge_eur_year")
        pso = row.pop("_pso", None)
        if standing and pso:
            try:
                row["standing_charge_eur_year"] = str(
                    round(float(standing) + pso, 2)
                )
            except (ValueError, TypeError):
                pass

        #find structure
        if row.get("unit_rate_weekday_day_eur_kwh"):
            row["structure"] = "WeekendPlan"
        elif row.get("unit_rate_peak_eur_kwh"):
            row["structure"] = "3-Period"
        elif row.get("unit_rate_night_eur_kwh"):
            row["structure"] = "DayNight"
        else:
            row["structure"] = "24h"
    #parse unit rates from rate table
    def _parse_unit_rates(
        self, table_text: str, row: dict[str, Any], meter_type: str
    ) -> None:
        lines = [l.strip() for l in table_text.splitlines() if l.strip()]

        for i, line in enumerate(lines):
            # day/night/peak
            if "Smart Day" in line and "EV" not in line:
                rate = self._find_rate_after(lines, i)
                if rate:
                    row["unit_rate_day_eur_kwh"] = rate

            elif "Smart Night" in line:
                rate = self._find_rate_after(lines, i)
                if rate:
                    row["unit_rate_night_eur_kwh"] = rate

            elif "Smart Peak" in line:
                rate = self._find_rate_after(lines, i)
                if rate:
                    row["unit_rate_peak_eur_kwh"] = rate

            #smart ev
            elif "EV Max 18 Hour" in line or "EV Max 18" in line:
                rate = self._find_rate_after(lines, i)
                if rate:
                    row["unit_rate_day_eur_kwh"] = rate

            elif "EV Max 6 Hour" in line or "EV Max 6" in line:
                rate = self._find_rate_after(lines, i)
                if rate:
                    row["unit_rate_night_eur_kwh"] = rate

            #24hr meter
            elif "24hr meter" in line and "Urban" not in line and "Rural" not in line:
                rate = self._find_rate_after(lines, i)
                if rate:
                    row["unit_rate_day_eur_kwh"] = rate
                    row["unit_rate_24h_eur_kwh"] = rate

            #weekend 6 period
            elif "Weekday Day" in line:
                rate = self._find_rate_after(lines, i)
                if rate:
                    row["unit_rate_weekday_day_eur_kwh"] = rate

            elif "Weekday Night" in line:
                rate = self._find_rate_after(lines, i)
                if rate:
                    row["unit_rate_weekday_night_eur_kwh"] = rate

            elif "Weekday Peak" in line:
                rate = self._find_rate_after(lines, i)
                if rate:
                    row["unit_rate_weekday_peak_eur_kwh"] = rate

            elif "Weekend Day" in line:
                rate = self._find_rate_after(lines, i)
                if rate:
                    row["unit_rate_weekend_day_eur_kwh"] = rate

            elif "Weekend Night" in line:
                rate = self._find_rate_after(lines, i)
                if rate:
                    row["unit_rate_weekend_night_eur_kwh"] = rate

            elif "Weekend Peak" in line:
                rate = self._find_rate_after(lines, i)
                if rate:
                    row["unit_rate_weekend_peak_eur_kwh"] = rate

    #parse weekend plan rates
    def _parse_weekend_rates(
            self, table_text: str, row: dict[str, Any]
    ) -> None:
        lines = [l.strip() for l in table_text.splitlines()]  # keep empty lines


        LABEL_MAP = {
            "Weekday Day": "unit_rate_weekday_day_eur_kwh",
            "Weekday Night": "unit_rate_weekday_night_eur_kwh",
            "Weekday Peak": "unit_rate_weekday_peak_eur_kwh",
            "Weekend Day": "unit_rate_weekend_day_eur_kwh",
            "Weekend Night": "unit_rate_weekend_night_eur_kwh",
            "Weekend Peak": "unit_rate_weekend_peak_eur_kwh",
        }

        for i, line in enumerate(lines):
            if line in LABEL_MAP:
                #disc rate offset by +3
                rate = self._find_rate_after(lines, i + 3)
                if rate:
                    row[LABEL_MAP[line]] = rate

    def _parse_standing_charge(
        self, table_text: str, row: dict[str, Any], meter_type: str
    ) -> None:
        lines = [l.strip() for l in table_text.splitlines() if l.strip()]

        #meter targeting
        plan_name = (row.get("plan_name") or "").lower()
        if "ev" in plan_name:
            targets = ["Urban Smart EV", "Urban Smart", "Urban 24hr"]
        elif meter_type == "smart":
            if "everyday" in plan_name:
                targets = ["Urban 24hr"]
            else:
                targets = ["Urban Smart", "Urban 24hr"]
        else:
            targets = ["Urban 24hr"]

        for target in targets:
            for i, line in enumerate(lines):
                if target in line:
                    for j in range(i, min(i + 3, len(lines))):
                        m = re.search(r"\u20ac([\d,.]+)", lines[j])
                        if m:
                            row["standing_charge_eur_year"] = m.group(1).replace(",", "")
                            return

        #last resort
        for i, line in enumerate(lines):
            if "Urban" in line:
                for j in range(i, min(i + 3, len(lines))):
                    m = re.search(r"\u20ac([\d,.]+)", lines[j])
                    if m:
                        row["standing_charge_eur_year"] = m.group(1).replace(",", "")
                        return

    @staticmethod
    def _find_rate_after(lines: list[str], start_idx: int) -> str | None:
        for j in range(start_idx, min(start_idx + 4, len(lines))):
            m = re.match(r"^(\d+\.\d+)$", lines[j])
            if m:
                return m.group(1)
        return None

    #helpers
    async def _click_filter(self, page: Page, label: str) -> None:
        try:
            btn = page.locator(f"button:has-text('{label}')").first
            if await btn.is_visible(timeout=2_000):
                await btn.click()
                await page.wait_for_timeout(1_500)
                logger.debug("Clicked filter: %s", label)
        except Exception:
            logger.warning("Could not click filter: %s", label)

    async def _dismiss_banners(self, page: Page) -> None:
        for selector in [
            "button:has-text('Accept All')",
            "button:has-text('Accept')",
            '#onetrust-accept-btn-handler',
            'button[id*="cookie"]',
        ]:
            try:
                btn = page.locator(selector).first
                if await btn.is_visible(timeout=3_000):
                    await btn.click()
                    await page.wait_for_timeout(500)
                    return
            except Exception:
                continue