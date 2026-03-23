# C2 – Usage Profiling Summary

**Run timestamp:** 2025-11-12T02:53:45
**Data window:** 2023-09-01 → 2025-12-31
**Timezone policy:** Process UTC, present Europe/Dublin.
**Cadence:** 30-minute intervals (48 slots/day); tiny gaps ffilled (limit=2).

## Artifacts
- Clean canonical table: `data/clean/clean_2023_2025_30min.csv`
- Typical day CSV/PNG: `reports/tables/typical_day.csv`, `reports/figures/typical_day_2023_2025.png`
- Weekday vs Weekend CSV/PNG: `reports/tables/weekday_weekend_profile.csv`, `reports/figures/weekday_weekend_overlay.png`
- Monthly totals CSV/PNG: `reports/tables/monthly_totals.csv`, `reports/figures/monthly_totals.png`
- Seasonal totals CSV: `reports/tables/seasonal_totals.csv` (optional)
- KPI table: `reports/tables/kpis.csv`

## Notes / Assumptions
- DST handled with `ambiguous=True` (choose first fall-back hour), spring gap shifted forward.
- Incomplete days (fair): 14.5% (first/last export day excluded; DST days allowed at 47/49 slots).
- Negative readings set to NaN and flagged (`flag_negative`).
- PII removed/upstream (no MPRN saved).

## Next (C3)
Use `data/clean/clean_2023_2025_30min.csv` with columns:
`timestamp_utc` (index), `timestamp_local`, `kWh`, `date`, `month`, `dow`, `is_weekend`, `flag_negative`.
