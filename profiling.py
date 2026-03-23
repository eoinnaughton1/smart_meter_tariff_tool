import pandas as pd
import numpy as np
import re
from io import StringIO
#expected times in ESB data
EXPECTED_TIMES = [f"{h:02d}:{m:02d}" for h in range(24) for m in (0, 30)]
TIME_RE = re.compile(r"^\s*\d{1,2}:\d{2}\s*$")

#ensure can be read multiple times, rewind file
def _reset_file(uploaded_file):

    try:
        uploaded_file.seek(0)
    except Exception:
        pass


def read_esb_wide(uploaded_file):
    def _read(**kw):
        _reset_file(uploaded_file)
        return pd.read_csv(
            uploaded_file,
            comment="#",
            dtype="string",
            skip_blank_lines=True,
            **kw
        )
    #try diff delimeters as esb files can differ
    for kw in [
        dict(sep=None, engine="python"),
        dict(sep="\t"),
        dict(sep=","),
        dict(delim_whitespace=True, engine="python"),
    ]:
        #ignore if fails
        try:
            df = _read(**kw)
            if df.shape[1] > 5:
                return df
        except Exception:
            pass

    raise ValueError("Unable to parse uploaded ESB file (no valid delimiter detected).")

#column names
def normalize_headers(df):
    df = df.copy()
    df.columns = (
        df.columns.astype(str)
        .str.replace("\ufeff", "", regex=False)
        .str.replace(r"\s+", " ", regex=True)
        .str.strip()
    )
    return df

#pick columns that look like times
def find_time_cols(cols):
    time_cols_raw = [c for c in cols if TIME_RE.match(str(c))]

    def to_hhmm(c):
        h, m = str(c).strip().split(":")
        return f"{int(h):02d}:{int(m):02d}"

    mapped = {c: to_hhmm(c) for c in time_cols_raw}
    mapped = {k: v for k, v in mapped.items() if v in EXPECTED_TIMES}
    return mapped

#guess date values from samp values
def detect_date_format(series, sample_size=50):
    vals = [
        str(x).strip()
        for x in series.dropna().astype(str).head(sample_size)
        if str(x).strip()
    ]
    candidates = [
        ("%Y-%m-%d", r"^\d{4}-\d{2}-\d{2}$"),
        ("%d-%m-%Y", r"^\d{2}-\d{2}-\d{4}$"),
        ("%d/%m/%Y", r"^\d{2}/\d{2}/\d{4}$"),
        ("%m/%d/%Y", r"^\d{2}/\d{2}/\d{4}$"),
        ("%Y/%m/%d", r"^\d{4}/\d{2}/\d{2}$"),
        ("%d.%m.%Y", r"^\d{2}\.\d{2}\.\d{4}$"),
    ]
    for fmt, pat in candidates:
        if all(re.match(pat, v) for v in vals):
            return fmt
    return None

#headers
def wide_to_long_strict(df_wide, dayfirst=False, verbose=False, file_hint=""):
    df_wide = normalize_headers(df_wide)
    cols = list(df_wide.columns)

    #same as notebook done previously
    if "Date" in cols:
        date_col = "Date"
    else:
        cands = [c for c in cols if c.lower() == "date"]
        if not cands:
            raise ValueError(f"[{file_hint}] No 'Date' column found.")
        date_col = cands[0]
    #detect date for parsing
    detected_fmt = detect_date_format(df_wide[date_col])
    if verbose:
        print(f"[{file_hint}] Detected date format:", detected_fmt or f"(none; dayfirst={dayfirst})")
    #standardise time colum
    time_map = find_time_cols(cols)
    if verbose:
        print(f"[{file_hint}] Found {len(time_map)}/48 time columns.")

    df_renamed = df_wide.rename(columns=time_map)
    use_times = [t for t in EXPECTED_TIMES if t in df_renamed.columns]
    #reshape from wide to long
    long = df_renamed.melt(
        id_vars=[date_col],
        value_vars=use_times,
        var_name="hhmm",
        value_name="kWh",
    )
    #ensure poper timestamp and kwh values
    date_str = long[date_col].astype("string").str.strip()

    if detected_fmt:
        date_only = pd.to_datetime(date_str, format=detected_fmt, errors="coerce")
        ts_text = date_only.dt.strftime("%Y-%m-%d") + " " + long["hhmm"]
        ts = pd.to_datetime(ts_text, format="%Y-%m-%d %H:%M", errors="coerce")
    else:
        ts = pd.to_datetime(date_str + " " + long["hhmm"], dayfirst=dayfirst, errors="coerce")

    long["timestamp"] = ts
    long["kWh"] = pd.to_numeric(long["kWh"], errors="coerce")
    long = long.dropna(subset=["timestamp"]).reset_index(drop=True)

    return long[["timestamp", "kWh"]]

#standardise smart meter data
def clean_esb_file(
    uploaded_file,
    dayfirst=True,
    timezone_local="Europe/Dublin",
    interval_minutes=30,
    gap_fill_limit_intervals=2,
    dedupe_keep="first",
    daily_completeness_min=44,
):

    dfw = read_esb_wide(uploaded_file)
    raw = wide_to_long_strict(dfw, dayfirst=dayfirst, verbose=False, file_hint="upload")

    raw = raw.copy()
    raw["timestamp"] = pd.to_datetime(raw["timestamp"], errors="coerce")
    raw = raw.dropna(subset=["timestamp"]).sort_values("timestamp")

    raw["timestamp_local"] = raw["timestamp"].dt.tz_localize(
        timezone_local,
        ambiguous=True,
        nonexistent="shift_forward",
    )
    raw["timestamp_utc"] = raw["timestamp_local"].dt.tz_convert("UTC")
    raw = raw.drop(columns=["timestamp"]).dropna(subset=["timestamp_utc"])

    raw = (
        raw.sort_values("timestamp_utc")
        .drop_duplicates(subset=["timestamp_utc"], keep=dedupe_keep)
    )

    interval = f"{interval_minutes}min"
    s = raw.set_index("timestamp_utc")["kWh"].sort_index().resample(interval).sum(min_count=1)
    s = s.ffill(limit=gap_fill_limit_intervals)

    df = pd.DataFrame({"kWh": s})
    df["timestamp_local"] = df.index.tz_convert(timezone_local)

    #neg flag
    neg_mask = df["kWh"] < 0
    df.loc[neg_mask, "kWh"] = np.nan
    df["flag_negative"] = neg_mask

    #helper
    local_naive = df["timestamp_local"].dt.tz_localize(None)
    df["date"] = local_naive.dt.date.astype("string")
    df["month"] = local_naive.dt.to_period("M").astype("string")
    df["dow"] = local_naive.dt.day_name()
    df["is_weekend"] = local_naive.dt.weekday >= 5

    #format friendly
    out = df.reset_index().rename(columns={"index": "timestamp_utc"})
    out = out.rename(columns={"timestamp_utc": "timestamp_utc"})
    return out


