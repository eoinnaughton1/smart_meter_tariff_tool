from __future__ import annotations
import logging
import re
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import pandas as pd

logger = logging.getLogger(__name__)

#column order
SCHEMA_COLUMNS = [
    "supplier",
    "plan_name",
    "meter_type",
    "unit_rate_day_eur_kwh",
    "unit_rate_night_eur_kwh",
    "unit_rate_peak_eur_kwh",
    "unit_rate_24h_eur_kwh",
    "standing_charge_eur_year",
    "discount_percent",
    "structure",
    "scraped_at",
    "unit_rate_weekday_day_eur_kwh",
    "unit_rate_weekday_night_eur_kwh",
    "unit_rate_weekday_peak_eur_kwh",
    "unit_rate_weekend_day_eur_kwh",
    "unit_rate_weekend_night_eur_kwh",
    "unit_rate_weekend_peak_eur_kwh",
]
#numeric columns
NUMERIC_COLUMNS = [
    "unit_rate_day_eur_kwh",
    "unit_rate_night_eur_kwh",
    "unit_rate_peak_eur_kwh",
    "unit_rate_24h_eur_kwh",
    "standing_charge_eur_year",
    "discount_percent",
    "unit_rate_weekday_day_eur_kwh",
    "unit_rate_weekday_night_eur_kwh",
    "unit_rate_weekday_peak_eur_kwh",
    "unit_rate_weekend_day_eur_kwh",
    "unit_rate_weekend_night_eur_kwh",
    "unit_rate_weekend_peak_eur_kwh",
]


def clean_numeric(raw: str | None) -> float | None:
    if raw is None:
        return None
    raw = str(raw).strip()
    if not raw or raw.lower() in ("n/a", "-", "\u2014", ""):
        return None
    #remove known numeric, currency & % signs
    cleaned = re.sub(r"[\u20ac%,]", "", raw)
    cleaned = re.sub(
        r"\s*(per|kwh|kWh|cent|year|yr|p\.a\.).*",
        "",
        cleaned,
        flags=re.IGNORECASE,
    )
    cleaned = cleaned.strip()
    try:
        return float(cleaned)
    except ValueError:
        logger.warning("Could not parse numeric value from: %r", raw)
        return None

#tariff type based on rates that are available
def classify_structure(meter_type: str, row: dict[str, Any]) -> str:
    mt = (meter_type or "").lower().strip()

    #3 period
    if "smart" in mt or "3" in mt or "tou" in mt:
        return "3-Period"

    #peak rate
    if row.get("unit_rate_peak_eur_kwh") is not None:
        return "3-Period"

    # Day/night
    if "night" in mt or "daynight" in mt:
        return "DayNight"
    if row.get("unit_rate_night_eur_kwh") is not None:
        return "DayNight"

    return "24h"

#base class for all supplier scrapers
class BaseTariffScraper(ABC):

    supplier_name: str = "Unknown"
    target_url: str = ""

    def __init__(self, output_dir: str | Path = "data/tariffs/clean"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    #subclass
    @abstractmethod
    async def scrape(self) -> pd.DataFrame:

        ...

    #format into consistent structure
    def standardise(self, df: pd.DataFrame) -> pd.DataFrame:

        now = datetime.now(timezone.utc).isoformat()
        df["scraped_at"] = now
        df["supplier"] = self.supplier_name


        for col in SCHEMA_COLUMNS:
            if col not in df.columns:
                df[col] = None

        #numeric columns
        for col in NUMERIC_COLUMNS:
            df[col] = df[col].apply(clean_numeric)

        #convert unit rates
        UNIT_RATE_COLS = [c for c in NUMERIC_COLUMNS if c.startswith("unit_rate")]
        for col in UNIT_RATE_COLS:
            df[col] = df[col] / 100

        #structure
        if "structure" not in df.columns or df["structure"].isna().all():
            df["structure"] = df.apply(
                lambda r: classify_structure(
                    r.get("meter_type", ""), r.to_dict()
                ),
                axis=1,
            )

        #normalise
        df["meter_type"] = df["meter_type"].apply(self._normalise_meter_type)

        return df[SCHEMA_COLUMNS]

    def save(self, df: pd.DataFrame) -> Path:
        slug = self.supplier_name.lower().replace(" ", "_")
        path = self.output_dir / f"{slug}_auto.csv"
        df.to_csv(path, index=False)
        logger.info("Saved %d tariffs -> %s", len(df), path)
        return path
    #wrapper
    def run_and_save(self) -> Path:
        import asyncio

        raw = asyncio.run(self.scrape())
        clean = self.standardise(raw)
        return self.save(clean)

    #helpers
    @staticmethod
    def _normalise_meter_type(raw: str | None) -> str:
        if raw is None:
            return "24h"
        mt = str(raw).lower().strip()
        if "smart" in mt or "tou" in mt or "3" in mt:
            return "smart"
        if "night" in mt or "day" in mt:
            return "daynight"
        return "24h"