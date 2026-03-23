from __future__ import annotations
from typing import Type
from scrapers.base import BaseTariffScraper
from scrapers.electric_ireland import ElectricIrelandScraper
from scrapers.bord_gais import BordGaisScraper
from scrapers.sse_airtricity import SseAirtricityScraper

#add in suppliers here
_REGISTRY: dict[str, Type[BaseTariffScraper]] = {
    "electric_ireland": ElectricIrelandScraper,
    "bord_gais": BordGaisScraper,
    "sse_airtricity": SseAirtricityScraper,
}


def get_scraper(
    supplier_key: str,
    output_dir: str = "data/tariffs/clean",
) -> BaseTariffScraper:
    #start scraper
    cls = _REGISTRY.get(supplier_key.lower())
    if cls is None:
        available = ", ".join(sorted(_REGISTRY.keys()))
        raise KeyError(
            f"Unknown supplier {supplier_key!r}. Available: {available}"
        )
    return cls(output_dir=output_dir)


def list_suppliers() -> list[str]:
    #return supplier keys
    return sorted(_REGISTRY.keys())