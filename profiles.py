import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pytz

#daily shaps
def base_profile_shape(profile_type):

    slots = np.arange(48)
    hours = slots / 2

    #typical household
    if profile_type == "Typical Household":
        shape = (
            0.6
            + 0.4 * np.exp(-0.5 * (hours - 7)**2)   #morning
            + 0.8 * np.exp(-0.5 * (hours - 18)**2)  #evening
        )

    # EV / night-heavy
    elif profile_type == "Night-Heavy (EV)":
        shape = (
            0.5
            + 1.2 * np.exp(-0.5 * (hours - 1)**2)   #night charging
            + 0.5 * np.exp(-0.5 * (hours - 18)**2)
        )

    # Peak-heavy
    elif profile_type == "Peak-Heavy":
        shape = (
            0.5
            + 1.5 * np.exp(-0.5 * (hours - 18)**2)
        )

    else:
        raise ValueError("Unknown profile type")

    #normalise
    shape = shape / shape.sum()

    return shape


#winter usage higher account for that
def seasonal_multiplier(day_of_year):
    return 1 + 0.25 * np.cos(2 * np.pi * (day_of_year - 1) / 365)


#create the profiles
def generate_profile(profile_type, annual_kwh, year=2024):

    tz = pytz.timezone("Europe/Dublin")

    start_date = datetime(year, 1, 1)
    end_date = datetime(year, 12, 31)

    days = (end_date - start_date).days + 1

    base_shape = base_profile_shape(profile_type)

    rows = []

    total_weight = 0

    #calc total seasonal
    for d in range(days):
        date = start_date + timedelta(days=d)
        mult = seasonal_multiplier(date.timetuple().tm_yday)
        total_weight += mult

    daily_base_kwh = annual_kwh / total_weight

    #build data
    for d in range(days):
        date = start_date + timedelta(days=d)
        day_of_year = date.timetuple().tm_yday
        mult = seasonal_multiplier(day_of_year)

        daily_kwh = daily_base_kwh * mult

        for slot in range(48):

            hour = slot // 2
            minute = 30 if slot % 2 else 0
            #timestamps
            ts_local = tz.localize(datetime(year, date.month, date.day, hour, minute))
            ts_utc = ts_local.astimezone(pytz.utc)

            kwh = daily_kwh * base_shape[slot]

            rows.append({
                "timestamp_utc": ts_utc,
                "timestamp_local": ts_local,
                "kWh": kwh
            })

    df = pd.DataFrame(rows)

    df["date"] = df["timestamp_local"].dt.date.astype(str)
    df["month"] = df["timestamp_local"].dt.to_period("M").astype(str)
    df["dow"] = df["timestamp_local"].dt.day_name()
    df["is_weekend"] = df["timestamp_local"].dt.weekday >= 5
    df["flag_negative"] = False

    return df
