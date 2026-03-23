# C2 – Usage Profiling Summary

**Run timestamp:** 2026-01-22T00:03:31
**Data window:** 2024-01-01 → 2024-12-31
**Timezone policy:** Process UTC, present Europe/Dublin.
**Cadence:** 30-minute intervals (48 slots/day); tiny gaps ffilled (limit=2).

## Artifacts
- Clean canonical table: `data/clean/clean_2024_30min.csv`
- Typical day CSV/PNG: `reports/tables/typical_day_2024.csv`, `reports/figures/typical_day_2024.png`
- Weekday vs Weekend CSV/PNG: `reports/tables/weekday_weekend_profile_2024.csv`, `reports/figures/weekday_weekend_overlay_2024.png`
- Monthly totals CSV/PNG: `reports/tables/monthly_totals_2024.csv`, `reports/figures/monthly_totals_2024.png`
- Seasonal totals CSV: `reports/tables/seasonal_totals_2024.csv` (optional)
- KPI table: `reports/tables/kpis_2024.csv`

## Notes / Assumptions
- DST handled with `ambiguous=True` (choose first fall-back hour), spring gap shifted forward.
- Incomplete days (fair): 0.0% (first/last export day excluded; DST days allowed at 47/49 slots).
- Negative readings set to NaN and flagged (`flag_negative`).
- PII removed/upstream (no MPRN saved).

## Next (C3)
Use `data/clean/clean_2023_2025_30min.csv` with columns:
`timestamp_utc` (index), `timestamp_local`, `kWh`, `date`, `month`, `dow`, `is_weekend`, `flag_negative`.
