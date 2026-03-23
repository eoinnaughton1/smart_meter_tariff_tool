from __future__ import annotations
import logging
import re
from typing import Any
import pandas as pd
from playwright.async_api import async_playwright, Page, Locator
from scrapers.base import BaseTariffScraper
#logger
logger = logging.getLogger(__name__)

#url
class ElectricIrelandScraper(BaseTariffScraper):
    supplier_name = "Electric Ireland"
    target_url = (
        "https://www.electricireland.ie/switch/new-customer/price-plans?priceType=E"
    )

    #selectors
    CARD_SELECTOR = "div.scrollable-card"
    CARD_FALLBACK = '[class*="scrollable-card"], [class*="plan-col"]'
    PRICING_BTN = "button.btn-price-info"
    PRICING_BTN_FALLBACK = 'button:has-text("Full pricing")'
    MODAL_SELECTOR = ".modal.show, .modal.in"
    MODAL_CLOSE = 'button[data-dismiss="modal"], button[data-bs-dismiss="modal"], .modal .close, .modal button:has-text("×")'

    #regex patterns
    MODAL_PATTERNS = {
        #day/standart rates
        "unit_rate_day_eur_kwh": re.compile(
            r"Effective\s+(?:Day\s+)?unit\s+price\s+with\s+[\d.]+%.*?"
            r"(\d+\.\d+)\s*c/kWh",
            re.I | re.S,
        ),
        #night rate
        "unit_rate_night_eur_kwh": re.compile(
            r"Effective\s+Night\s+unit.*?(\d+\.\d+)\s*c/kWh",
            re.I | re.S,
        ),
        #peak rate
        "unit_rate_peak_eur_kwh": re.compile(
            r"Effective\s+Peak\s+unit.*?(\d+\.\d+)\s*c/kWh",
            re.I | re.S,
        ),
        #standing charge
        "standing_charge_eur_year": re.compile(
            r"Standing\s+charge\s+.*?urban.*?"
            r"\u20ac([\d,]+\.?\d*)",
            re.I | re.S,
        ),
        #PSO levy
        "pso_levy_eur_year": re.compile(
            r"Public\s+Service\s+Obligation.*?"
            r"\u20ac([\d,]+\.?\d*)",
            re.I | re.S,
        ),
    }

    #fallback
    FALLBACK_UNIT_RATE = re.compile(
        r"Standard\s+unit\s+price\s+(\d+\.\d+)\s*c/kWh", re.I
    )

    #capture 24h rate
    EFFECTIVE_24H_RATE = re.compile(
        r"Effective\s+unit\s+price\s+with\s+[\d.]+%.*?"
        r"(\d+\.\d+)\s*c/kWh",
        re.I | re.S,
    )

    #reveal subset cards
    METER_FILTERS = ["Standard Meter", "Day & Night Meter", "Smart Meter"]
    FILTER_BTN_SELECTOR = "button.filter"

    #public interfsce
    async def scrape(self) -> pd.DataFrame:
        """Launch headless Chromium, scrape all plans via modal, return raw DF."""
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
                    self.target_url, wait_until="networkidle", timeout=60_000
                )

                await self._dismiss_banners(page)

                rows: list[dict[str, Any]] = []

                #cycle meter types and reveal plans
                for meter_filter in self.METER_FILTERS:
                    filter_rows = await self._scrape_filter(page, meter_filter)
                    rows.extend(filter_rows)

                logger.info("Extracted %d total tariff rows", len(rows))
                return pd.DataFrame(rows)

            finally:
                await browser.close()
    #scrape visible cards from meter type button
    async def _scrape_filter(
        self, page: Page, meter_filter: str
    ) -> list[dict[str, Any]]:
        logger.info("Selecting filter: %s", meter_filter)

        #click matching button
        filter_btn = page.locator(
            f'{self.FILTER_BTN_SELECTOR}:has-text("{meter_filter}")'
        )
        try:
            await filter_btn.click()
            await page.wait_for_timeout(1_000)  # let cards re-render
        except Exception:
            logger.warning("Could not click filter: %s", meter_filter)
            return []

        #find visible cards
        all_cards = page.locator(self.CARD_SELECTOR)
        count = await all_cards.count()
        logger.info("Filter '%s': %d total cards in DOM", meter_filter, count)

        rows: list[dict[str, Any]] = []
        for i in range(count):
            card = all_cards.nth(i)
            #skip hidden cards
            try:
                if not await card.is_visible(timeout=500):
                    continue
            except Exception:
                continue

            try:
                row = await self._extract_plan(page, card, index=i)
                if row and row.get("plan_name"):
                    rows.append(row)
            except Exception:
                logger.exception(
                    "Failed to extract card %d under filter %s", i, meter_filter
                )

        logger.info("Filter '%s': extracted %d plans", meter_filter, len(rows))
        return rows

    #page helpers
    async def _dismiss_banners(self, page: Page) -> None:
        for selector in [
            "button:has-text('Accept All')",
            "button:has-text('Accept')",
            "button:has-text('Got it')",
            '[id*="cookie"] button',
            '[class*="cookie"] button',
        ]:
            try:
                btn = page.locator(selector).first
                if await btn.is_visible(timeout=2_000):
                    await btn.click()
                    await page.wait_for_timeout(500)
                    logger.debug("Dismissed banner via %s", selector)
                    return
            except Exception:
                continue
    #locate plan card containers
    async def _find_cards(self, page: Page) -> list[Locator]:
        cards = page.locator(self.CARD_SELECTOR)
        count = await cards.count()
        if count > 0:
            logger.info("Found %d cards via primary selector", count)
            return [cards.nth(i) for i in range(count)]

        #Fallback
        logger.warning("Primary selector found 0 cards, trying fallback")
        cards = page.locator(self.CARD_FALLBACK)
        count = await cards.count()
        logger.info("Fallback found %d cards", count)
        return [cards.nth(i) for i in range(count)]

    #per plan extraction
    async def _extract_plan(
        self, page: Page, card: Locator, index: int
    ) -> dict[str, Any]:
        row: dict[str, Any] = {}

        #surface data
        row["plan_name"] = await self._get_plan_name(card)
        row["meter_type"] = await self._get_meter_type(card)
        row["discount_percent"] = await self._get_discount(card)

        logger.info(
            "Plan %d: %s (%s, %s%% discount)",
            index,
            row["plan_name"],
            row["meter_type"],
            row["discount_percent"],
        )

        #pricing model opened
        modal_text = await self._open_pricing_modal(page, card, index)
        if modal_text:
            self._parse_modal_rates(modal_text, row)
        else:
            logger.warning("No modal text for plan %d, using card surface only", index)
            await self._extract_surface_rate(card, row)

        return row
    #extract plan name
    async def _get_plan_name(self, card: Locator) -> str | None:
        try:
            btn = card.locator("button.btn-switch, button:has-text('Sign Up')").first
            aria = await btn.get_attribute("aria-label")
            if aria and "," in aria:
                #format
                name = aria.split(",", 1)[1].strip()
                if name:
                    return name
        except Exception:
            pass

        #elements inside top cards headings
        for sel in ("h2", "h3", "h4", ".top-card h2", ".top-card h3"):
            try:
                loc = card.locator(sel).first
                if await loc.is_visible(timeout=500):
                    text = (await loc.inner_text()).strip()
                    if text and len(text) > 3:
                        return text
            except Exception:
                continue

        #scan card text
        try:
            text = await card.inner_text()
            m = re.search(
                r"(EnergySaver.*?|Home Electric.*?|NightSaver.*?|Nightboost.*?|Night.*?Saver.*?)(?:\n|$)",
                text, re.I,
            )
            if m:
                return m.group(1).strip()
        except Exception:
            pass

        return None
    #extract meter type
    async def _get_meter_type(self, card: Locator) -> str | None:
        mt = await card.get_attribute("data-meter-type")
        if mt:
            return mt.strip()

        # fallback
        try:
            text = await card.inner_text()
            m = re.search(
                r"(Smart\s*Meter|Standard\s*Meter|Day\s*&?\s*Night(?:\s*Meter)?|24\s*hour)",
                text, re.I,
            )
            if m:
                return m.group(1).strip()
        except Exception:
            pass
        return None
    #extract headlines discount
    async def _get_discount(self, card: Locator) -> str | None:
        try:
            btn = card.locator("button.btn-switch, button:has-text('Sign Up')").first
            aria = await btn.get_attribute("aria-label")
            if aria:
                m = re.search(r"(\d+)\s*%", aria)
                if m:
                    return m.group(1)
        except Exception:
            pass

        #fallback
        try:
            text = await card.inner_text()
            #look for discount
            m = re.search(r"(\d{1,2})\s*%", text)
            if m:
                return m.group(1)
        except Exception:
            pass
        return "0"

#modal interaction
    async def _open_pricing_modal(
        self, page: Page, card: Locator, index: int
    ) -> str | None:
        try:
            #find pricing buttom for card
            btn = card.locator(self.PRICING_BTN).first
            if not await btn.is_visible(timeout=2_000):
                btn = card.locator(self.PRICING_BTN_FALLBACK).first

            #click button
            await btn.scroll_into_view_if_needed()
            await btn.click()

            #wait till visible
            modal = page.locator(self.MODAL_SELECTOR).first
            await modal.wait_for(state="visible", timeout=5_000)
            await page.wait_for_timeout(500)  # let content render

            #extract text
            modal_text = await modal.inner_text()
            logger.debug("Modal text for plan %d: %d chars", index, len(modal_text))

            #close
            await self._close_modal(page)

            return modal_text

        except Exception:
            logger.warning("Could not open/read pricing modal for plan %d", index)
            #try close stuck modal
            await self._close_modal(page)
            return None
    #close open bootstrap
    async def _close_modal(self, page: Page) -> None:
        try:
            #close button
            close = page.locator(self.MODAL_CLOSE).first
            if await close.is_visible(timeout=1_000):
                await close.click()
                await page.wait_for_timeout(500)
                return
        except Exception:
            pass

        #fallback
        try:
            await page.keyboard.press("Escape")
            await page.wait_for_timeout(500)
        except Exception:
            pass

        #click backdrop fallback
        try:
            await page.locator(".modal-backdrop").click()
            await page.wait_for_timeout(500)
        except Exception:
            pass

    #modal text parsing
    #extract nam via regex
    def _parse_modal_rates(self, modal_text: str, row: dict[str, Any]) -> None:
        #extract via regex
        for field, pattern in self.MODAL_PATTERNS.items():
            m = pattern.search(modal_text)
            if m:
                value = m.group(1).replace(",", "")
                row[field] = value

        #determine if flat or not
        has_day = row.get("unit_rate_day_eur_kwh")
        has_night = row.get("unit_rate_night_eur_kwh")
        has_peak = row.get("unit_rate_peak_eur_kwh")

        #for standard metr show
        if not has_day and not has_night and not has_peak:
            m = self.EFFECTIVE_24H_RATE.search(modal_text)
            if m:
                row["unit_rate_24h_eur_kwh"] = m.group(1)
                #set as day
                row["unit_rate_day_eur_kwh"] = m.group(1)

        #if for multi plan
        if has_day and not row.get("unit_rate_24h_eur_kwh"):
            row["unit_rate_24h_eur_kwh"] = None

        #SC and PSO levy
        standing = row.get("standing_charge_eur_year")
        pso = row.get("pso_levy_eur_year")
        if standing and pso:
            try:
                total = float(standing) + float(pso)
                row["standing_charge_eur_year"] = str(round(total, 2))
            except (ValueError, TypeError):
                pass
        #remove pso levy from row
        row.pop("pso_levy_eur_year", None)

        #take structure
        if has_peak:
            row["structure"] = "3-Period"
        elif has_night:
            row["structure"] = "DayNight"
        else:
            row["structure"] = "24h"
            if has_day:
                row["unit_rate_24h_eur_kwh"] = row["unit_rate_day_eur_kwh"]
    #fallback extract single visible rate
    async def _extract_surface_rate(
        self, card: Locator, row: dict[str, Any]
    ) -> None:
        try:
            text = await card.inner_text()
            m = re.search(r"(\d+\.\d+)c\s*per\s*kWh", text, re.I)
            if m:
                rate = m.group(1)
                row["unit_rate_day_eur_kwh"] = rate
                row["unit_rate_24h_eur_kwh"] = rate
        except Exception:
            pass