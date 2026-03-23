import pandas as pd
import numpy as np
import json
import warnings

#time boundaries for tariffs
TARIFF_WINDOWS = {
    "day_night": {
        "night_start": 23,
        "night_end": 8,
    },
    "three_period": {
        "night_start": 23,
        "night_end": 8,
        "peak_start": 17,
        "peak_end": 19,
    },
}

#night boundary includes after midnight
def assign_period(ts_local, structure):
    hour = ts_local.hour

    if structure == "24h":
        return "day"

    if structure == "DayNight":
        ns = TARIFF_WINDOWS["day_night"]["night_start"]
        ne = TARIFF_WINDOWS["day_night"]["night_end"]
        return "night" if (hour >= ns or hour < ne) else "day"
#checks night first, then peak, then says day
    if structure == "3-Period":
        ns = TARIFF_WINDOWS["three_period"]["night_start"]
        ne = TARIFF_WINDOWS["three_period"]["night_end"]
        ps = TARIFF_WINDOWS["three_period"]["peak_start"]
        pe = TARIFF_WINDOWS["three_period"]["peak_end"]

        if hour >= ns or hour < ne:
            return "night"
        if ps <= hour < pe:
            return "peak"
        return "day"
    #weekend plans including tou
    if structure == "WeekendPlan":  # <-- same indentation as "3-Period"
        ns = TARIFF_WINDOWS["three_period"]["night_start"]
        ne = TARIFF_WINDOWS["three_period"]["night_end"]
        ps = TARIFF_WINDOWS["three_period"]["peak_start"]
        pe = TARIFF_WINDOWS["three_period"]["peak_end"]

        is_weekend = ts_local.weekday() >= 5
        prefix = "weekend" if is_weekend else "weekday"

        if hour >= ns or hour < ne:
            return f"{prefix}_night"
        if ps <= hour < pe:
            return f"{prefix}_peak"
        return f"{prefix}_day"

    raise ValueError(f"Unknown structure: {structure}")


#assign perid to each tariff row and group
def compute_usage_by_period(usage_df, structure):
    df = usage_df.copy()
    df["period"] = df["timestamp_local"].apply(
        assign_period, structure=structure
    )
#keeps labels for later
    grouped = df.groupby("period")["kWh"].sum()
    grouped = grouped.reindex(
        ["day", "night", "peak",
         "weekday_day", "weekday_night", "weekday_peak",
         "weekend_day", "weekend_night", "weekend_peak"],
        fill_value=0.0
    )

    return grouped


#special rules, cam come as blank, JSON, or parsed list
def _parse_special_rules(tariff_row):
    raw = tariff_row.get("special_rules", None)

    if raw is None or (isinstance(raw, float) and np.isnan(raw)):
        return []
    if isinstance(raw, list):
        return raw
    if isinstance(raw, str):
        raw = raw.strip()
        if not raw:
            return []
        try:
            parsed = json.loads(raw)
            return parsed if isinstance(parsed, list) else [parsed]
        except json.JSONDecodeError:
            warnings.warn(f"Could not parse special_rules: {raw!r}")
            return []
    return []

#apply the rules in ordder
def _apply_special_rules(usage_df, tariff_row, energy_cost):
    rules = _parse_special_rules(tariff_row)
    if not rules:
        return 0.0, []

    rules = sorted(rules, key=lambda r: r.get("priority", 99))

    savings = 0.0
    applied = []

    for rule in rules:
        rule_type = rule.get("type", "")

        if rule_type == "free_weekend":
            weekend_kwh = usage_df.loc[
                usage_df["is_weekend"], "kWh"
            ].sum()
            #estimate rate
            avg_rate = average_rate(tariff_row)
            weekend_saving = weekend_kwh * avg_rate
            discount = tariff_row.get("discount_percent", 0) or 0
            weekend_saving *= (1 - discount / 100)
            savings += weekend_saving
            applied.append(
                f"Free weekend electricity: saved on {weekend_kwh:.0f} kWh"
            )
        #filter matching rows like above
        elif rule_type == "free_hours":
            start_h = rule.get("start", 0)
            end_h = rule.get("end", 24)
            days = rule.get("days", None)

            mask = (
                (usage_df["timestamp_local"].dt.hour >= start_h)
                & (usage_df["timestamp_local"].dt.hour < end_h)
            )
            if days:
                mask &= usage_df["dow"].isin(days)

            free_kwh = usage_df.loc[mask, "kWh"].sum()
            avg_rate = average_rate(tariff_row)
            discount = tariff_row.get("discount_percent", 0) or 0
            hour_saving = free_kwh * avg_rate * (1 - discount / 100)
            savings += hour_saving
            applied.append(
                f"Free hours ({start_h}:00-{end_h}:00): saved on {free_kwh:.0f} kWh"
            )
        #nigt boost option
        elif rule_type == "ev_night_boost":
            start_h = rule.get("start", 2)
            end_h = rule.get("end", 5)
            boost_rate = rule.get("rate", 0.0)

            mask = (
                (usage_df["timestamp_local"].dt.hour >= start_h)
                & (usage_df["timestamp_local"].dt.hour < end_h)
            )
            boost_kwh = usage_df.loc[mask, "kWh"].sum()
            normal_night = tariff_row.get("unit_rate_night_eur_kwh", 0) or 0
            rate_diff = normal_night - boost_rate
            if rate_diff > 0:
                savings += boost_kwh * rate_diff
                applied.append(
                    f"EV night boost ({start_h}:00-{end_h}:00): "
                    f"reduced rate on {boost_kwh:.0f} kWh"
                )
        #cacshback
        elif rule_type == "cashback":
            amount = rule.get("amount", 0)
            savings += amount
            applied.append(f"Cashback/bonus: -\u20ac{amount:.0f}")

        else:
            warnings.warn(f"Unknown special rule type: {rule_type!r}")

    return savings, applied

#average rate
def average_rate(tariff_row):
    rates = []
    for col in [
        "unit_rate_24h_eur_kwh",
        "unit_rate_day_eur_kwh",
        "unit_rate_night_eur_kwh",
        "unit_rate_peak_eur_kwh",
        "unit_rate_weekday_day_eur_kwh",
        "unit_rate_weekday_night_eur_kwh",
        "unit_rate_weekday_peak_eur_kwh",
        "unit_rate_weekend_day_eur_kwh",
        "unit_rate_weekend_night_eur_kwh",
        "unit_rate_weekend_peak_eur_kwh",
    ]:
        val = tariff_row.get(col)
        if val is not None and not (isinstance(val, float) and np.isnan(val)):
            rates.append(val)
    return np.mean(rates) if rates else 0.0


#splits tariff persiods
def cost_plan(usage_df, tariff_row):
    structure = tariff_row["structure"]
    usage_split = compute_usage_by_period(usage_df, structure)

    #usage splits
    total_kwh_check = usage_split.sum()
    if not np.isclose(total_kwh_check, usage_df["kWh"].sum(), rtol=1e-4):
        warnings.warn("Usage split does not sum to total kWh")

    #raw energy
    if structure == "24h":
        rate = tariff_row.get("unit_rate_24h_eur_kwh")
        if rate is None or (isinstance(rate, float) and np.isnan(rate)):
            rate = tariff_row.get("unit_rate_day_eur_kwh", 0)
        energy_cost = usage_split["day"] * rate

    elif structure == "DayNight":
        energy_cost = (
            usage_split["day"] * tariff_row["unit_rate_day_eur_kwh"]
            + usage_split["night"] * tariff_row["unit_rate_night_eur_kwh"]
        )


    elif structure == "3-Period":

        energy_cost = (

                usage_split["day"] * tariff_row["unit_rate_day_eur_kwh"]

                + usage_split["night"] * tariff_row["unit_rate_night_eur_kwh"]

                + usage_split["peak"] * tariff_row["unit_rate_peak_eur_kwh"]

        )


    elif structure == "WeekendPlan":

        energy_cost = (

                usage_split["weekday_day"] * tariff_row["unit_rate_weekday_day_eur_kwh"]

                + usage_split["weekday_night"] * tariff_row["unit_rate_weekday_night_eur_kwh"]

                + usage_split["weekday_peak"] * tariff_row["unit_rate_weekday_peak_eur_kwh"]

                + usage_split["weekend_day"] * tariff_row["unit_rate_weekend_day_eur_kwh"]

                + usage_split["weekend_night"] * tariff_row["unit_rate_weekend_night_eur_kwh"]

                + usage_split["weekend_peak"] * tariff_row["unit_rate_weekend_peak_eur_kwh"]

        )


    else:

        raise ValueError(f"Unsupported structure: {structure}")

    #apply the discount
    discount = tariff_row.get("discount_percent", 0)
    if discount is None or (isinstance(discount, float) and np.isnan(discount)):
        discount = 0

    #see what discount applies to
    discount_target = tariff_row.get("discount_applies_to", "energy")
    if discount_target is None or (isinstance(discount_target, float) and np.isnan(discount_target)):
        discount_target = "energy"

    #standing charge
    standing = tariff_row.get("standing_charge_eur_year", 0)
    if standing is None or (isinstance(standing, float) and np.isnan(standing)):
        standing = 0

    #combine
    if discount_target == "total":
        total_cost = (energy_cost + standing) * (1 - discount / 100)
    elif discount_target == "none" or discount == 0:
        total_cost = energy_cost + standing
    else:
        #disc on energy only (default)
        discounted_cost = energy_cost * (1 - discount / 100)
        total_cost = discounted_cost + standing

    #rules
    special_savings, _ = _apply_special_rules(
        usage_df, tariff_row, energy_cost
    )
    total_cost -= special_savings

    return total_cost

#per period. Keep pre discount costs seperate
def cost_breakdown(usage_df, tariff_row):
    structure = tariff_row["structure"]
    usage_split = compute_usage_by_period(usage_df, structure)

    breakdown = {}


    if structure == "24h":
        rate = tariff_row.get("unit_rate_24h_eur_kwh")
        if rate is None or (isinstance(rate, float) and np.isnan(rate)):
            rate = tariff_row.get("unit_rate_day_eur_kwh", 0)
        breakdown["energy (24h)"] = usage_split["day"] * rate

    elif structure == "DayNight":
        breakdown["energy (day)"] = (
                usage_split["day"] * tariff_row["unit_rate_day_eur_kwh"]
        )
        breakdown["energy (night)"] = (
                usage_split["night"] * tariff_row["unit_rate_night_eur_kwh"]
        )

    elif structure == "3-Period":
        breakdown["energy (day)"] = (
                usage_split["day"] * tariff_row["unit_rate_day_eur_kwh"]
        )
        breakdown["energy (night)"] = (
                usage_split["night"] * tariff_row["unit_rate_night_eur_kwh"]
        )
        breakdown["energy (peak)"] = (
                usage_split["peak"] * tariff_row["unit_rate_peak_eur_kwh"]
        )


    elif structure == "3-Period":

        breakdown["energy (day)"] = (

                usage_split["day"] * tariff_row["unit_rate_day_eur_kwh"]

        )

        breakdown["energy (night)"] = (

                usage_split["night"] * tariff_row["unit_rate_night_eur_kwh"]

        )

        breakdown["energy (peak)"] = (

                usage_split["peak"] * tariff_row["unit_rate_peak_eur_kwh"]

        )


    elif structure == "WeekendPlan":

        breakdown["energy (weekday day)"] = usage_split["weekday_day"] * tariff_row["unit_rate_weekday_day_eur_kwh"]

        breakdown["energy (weekday night)"] = usage_split["weekday_night"] * tariff_row[
            "unit_rate_weekday_night_eur_kwh"]

        breakdown["energy (weekday peak)"] = usage_split["weekday_peak"] * tariff_row["unit_rate_weekday_peak_eur_kwh"]

        breakdown["energy (weekend day)"] = usage_split["weekend_day"] * tariff_row["unit_rate_weekend_day_eur_kwh"]

        breakdown["energy (weekend night)"] = usage_split["weekend_night"] * tariff_row[
            "unit_rate_weekend_night_eur_kwh"]

        breakdown["energy (weekend peak)"] = usage_split["weekend_peak"] * tariff_row["unit_rate_weekend_peak_eur_kwh"]

    #discounts
    gross_energy = sum(breakdown.values())
    discount = tariff_row.get("discount_percent", 0)
    if discount is None or (isinstance(discount, float) and np.isnan(discount)):
        discount = 0

    discount_target = tariff_row.get("discount_applies_to", "energy")
    if discount_target is None or (isinstance(discount_target, float) and np.isnan(discount_target)):
        discount_target = "energy"

    #standing charges
    standing = tariff_row.get("standing_charge_eur_year", 0)
    if standing is None or (isinstance(standing, float) and np.isnan(standing)):
        standing = 0
    if standing > 0:
        breakdown["standing charge"] = standing

    #apply discount
    if discount > 0:
        if discount_target == "total":
            discount_amount = (gross_energy + standing) * discount / 100
        else:
            #default disc on energy
            discount_amount = gross_energy * discount / 100
        breakdown["discount saving"] = -discount_amount

    #special riles
    special_savings, applied = _apply_special_rules(
        usage_df, tariff_row, gross_energy
    )
    if special_savings > 0:
        breakdown["special savings"] = -special_savings

    ##total
    breakdown["total"] = sum(breakdown.values())

    return breakdown


#matching function
def match_tariffs(usage_df, tariffs_df):
    results = []

    total_kwh = usage_df["kWh"].sum()

    for _, row in tariffs_df.iterrows():
        try:
            est_cost = cost_plan(usage_df, row)

            # breakdown from summary
            bd = cost_breakdown(usage_df, row)

            #cost per kWh
            cost_per_kwh = est_cost / total_kwh if total_kwh > 0 else 0

            results.append({
                "supplier": row["supplier"],
                "plan_name": row["plan_name"],
                "meter_type": row.get("meter_type", ""),
                "structure": row["structure"],
                "estimated_cost_eur": round(est_cost, 2),
                "energy_cost_eur": round(
                    sum(v for k, v in bd.items()
                        if k.startswith("energy")), 2
                ),
                "discount_saving_eur": round(
                    bd.get("discount saving", 0), 2
                ),
                "standing_charge_eur": round(
                    bd.get("standing charge", 0), 2
                ),
                "special_savings_eur": round(
                    bd.get("special savings", 0), 2
                ),
                "cost_per_kwh_cent": round(cost_per_kwh * 100, 2),
            })

        except Exception as e:
            warnings.warn(f"Skipping {row.get('plan_name', '?')}: {e}")

    results_df = pd.DataFrame(results)
    results_df = results_df.sort_values(
        "estimated_cost_eur"
    ).reset_index(drop=True)

    return results_df