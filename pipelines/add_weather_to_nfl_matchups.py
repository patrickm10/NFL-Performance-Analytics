import polars as pl
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from timezonefinder import TimezoneFinder

FILE_IN = "backend/static/data/nfl_metadata/nfl_matchups_enriched.csv"

tf = TimezoneFinder()

def tz_from_latlon(lat: float, lon: float) -> str:
    tz = tf.timezone_at(lat=float(lat), lng=float(lon))
    return tz or "UTC"

def parse_kickoff_local(date_str: str, time_str: str, tz_name: str) -> datetime | None:
    s = str(time_str).strip().upper().replace(" ", "")
    fmts = [
        "%Y-%m-%d%I:%M%p",
        "%Y-%m-%d %I:%M%p",
        "%Y-%m-%d %H:%M",
        "%m/%d/%Y%I:%M%p",
        "%m/%d/%Y %I:%M%p",
    ]
    for fmt in fmts:
        try:
            needs_space = " " in fmt and " " not in f"{date_str}"
            dt = datetime.strptime(f"{date_str}{' ' if needs_space else ''}{s}", fmt)
            return dt.replace(tzinfo=ZoneInfo(tz_name))
        except Exception:
            continue
    return None

def round_to_nearest_hour(dt: datetime) -> datetime:
    if dt.minute >= 30:
        dt = dt + timedelta(hours=1)
    return dt.replace(minute=0, second=0, microsecond=0)

def fetch_open_meteo_hour(lat: float, lon: float, local_dt: datetime, tz_name: str) -> dict:
    date_str = local_dt.strftime("%Y-%m-%d")
    target_iso = round_to_nearest_hour(local_dt).strftime("%Y-%m-%dT%H:00")
    url = (
        "https://archive-api.open-meteo.com/v1/era5"
        f"?latitude={lat}&longitude={lon}"
        f"&start_date={date_str}&end_date={date_str}"
        "&hourly=temperature_2m,precipitation,relative_humidity_2m,pressure_msl,wind_speed_10m"
        f"&timezone={tz_name}"
    )
    try:
        r = requests.get(url, timeout=8)
        r.raise_for_status()
        data = r.json()
        h = data.get("hourly", {})
        times = h.get("time", [])
        if not times:
            return {"temp_C": None, "precip_mm": None, "wind_kph": None, "rel_humidity": None, "pressure_hpa": None}
        try:
            idx = times.index(target_iso)
        except ValueError:
            return {"temp_C": None, "precip_mm": None, "wind_kph": None, "rel_humidity": None, "pressure_hpa": None}
        temp_C = h.get("temperature_2m", [None])[idx]
        precip_mm = h.get("precipitation", [None])[idx]
        wind_ms = h.get("wind_speed_10m", [None])[idx]
        rh = h.get("relative_humidity_2m", [None])[idx]
        p_msl = h.get("pressure_msl", [None])[idx]
        wind_kph = wind_ms * 3.6 if wind_ms is not None else None
        pressure_hpa = p_msl
        return {
            "temp_C": temp_C,
            "precip_mm": precip_mm,
            "wind_kph": wind_kph,
            "rel_humidity": rh,
            "pressure_hpa": pressure_hpa,
        }
    except Exception:
        return {"temp_C": None, "precip_mm": None, "wind_kph": None, "rel_humidity": None, "pressure_hpa": None}

def process_row(rec: dict) -> dict:
    lat = float(rec["latitude"])
    lon = float(rec["longitude"])
    tz_name = tz_from_latlon(lat, lon)
    kickoff = parse_kickoff_local(str(rec["Date"]), str(rec["Time"]), tz_name)
    if kickoff is None:
        wx = {"temp_C": None, "precip_mm": None, "wind_kph": None, "rel_humidity": None, "pressure_hpa": None}
    else:
        wx = fetch_open_meteo_hour(lat, lon, kickoff, tz_name)
    return {
        "city": rec["city"],
        "state": rec.get("state"),
        "stadium_name": rec.get("stadium_name"),
        "Date": rec["Date"],
        "Time": rec["Time"],
        "latitude": lat,
        "longitude": lon,
        "timezone": tz_name,
        **wx,
    }

def main():
    df = pl.read_csv(FILE_IN)
    cols = ["Date", "Time", "city", "state", "stadium_name", "latitude", "longitude"]
    subset = (
        df.select(cols)
          .drop_nulls(["Date", "Time", "latitude", "longitude"])
          .to_dicts()
    )
    if not subset:
        print("No valid rows")
        return

    out = []
    with ThreadPoolExecutor(max_workers=5) as ex:
        futures = [ex.submit(process_row, rec) for rec in subset]
        for f in as_completed(futures):
            out.append(f.result())

    result_df = pl.from_dicts(out).select([
    "city", "state", "stadium_name", "Date", "Time",
    "latitude", "longitude","timezone", "temp_C", "precip_mm", 
    "wind_kph", "rel_humidity", "pressure_hpa"
])


    print(result_df)

    result_df.write_csv("backend/static/data/nfl_metadata/nfl_matchups_with_weather.csv")

if __name__ == "__main__":
    main()
