import requests
import logging
from datetime import datetime

logging.basicConfig(
    filename='weather_debug.log',
    level=logging.DEBUG,
    format='%(asctime)s %(levelname)s %(message)s'
)

_NWS_BASE = "https://api.weather.gov"
_NWS_HEADERS = {
    "User-Agent": "public_data/1.0 (github.com/z0mbi3jesus/public_data)",
    "Accept": "application/geo+json",
}

# Cached after first successful resolve — stable per grid point.
_nws_hourly_url_cache: dict = {}


def _resolve_nws_hourly_url(lat: float, lon: float) -> str | None:
    """Return the NWS hourly forecast URL for a coordinate pair, caching the result."""
    key = (round(lat, 4), round(lon, 4))
    if key in _nws_hourly_url_cache:
        return _nws_hourly_url_cache[key]
    try:
        resp = requests.get(
            f"{_NWS_BASE}/points/{lat},{lon}",
            headers=_NWS_HEADERS,
            timeout=10,
        )
        resp.raise_for_status()
        url = resp.json().get("properties", {}).get("forecastHourly")
        if url:
            _nws_hourly_url_cache[key] = url
        return url
    except Exception as exc:
        logging.warning(f"NWS points lookup failed for ({lat},{lon}): {exc}")
        return None


def fetch_nws_alerts(lat: float, lon: float) -> list[dict]:
    """Return active NWS weather alerts for the given coordinates as flat data points."""
    try:
        resp = requests.get(
            f"{_NWS_BASE}/alerts/active",
            params={"point": f"{lat},{lon}"},
            headers=_NWS_HEADERS,
            timeout=10,
        )
        resp.raise_for_status()
        features = resp.json().get("features", [])
    except Exception as exc:
        logging.warning(f"NWS alerts fetch failed for ({lat},{lon}): {exc}")
        print(f"[!] NWS alerts fetch failed: {exc}")
        return []

    points = []
    location = f"{lat},{lon}"
    for feature in features:
        props = feature.get("properties", {})
        ts = datetime.now().isoformat()
        for metric, value in [
            ("nws_alert_event",    props.get("event")),
            ("nws_alert_severity", props.get("severity")),
            ("nws_alert_urgency",  props.get("urgency")),
            ("nws_alert_certainty",props.get("certainty")),
            ("nws_alert_onset",    props.get("onset")),
            ("nws_alert_expires",  props.get("expires")),
            ("nws_alert_headline", props.get("headline")),
            ("nws_alert_status",   props.get("status")),
            ("nws_alert_area",     props.get("areaDesc")),
        ]:
            points.append({
                "timestamp": ts,
                "location": location,
                "metric": metric,
                "value": str(value) if value is not None else "",
                "unit": "",
            })
    return points


def fetch_nws_hourly(lat: float, lon: float) -> list[dict]:
    """Return the next 12 hourly NWS forecast periods as flat data points."""
    hourly_url = _resolve_nws_hourly_url(lat, lon)
    if not hourly_url:
        return []
    try:
        resp = requests.get(hourly_url, headers=_NWS_HEADERS, timeout=10)
        resp.raise_for_status()
        periods = resp.json().get("properties", {}).get("periods", [])[:12]
    except Exception as exc:
        logging.warning(f"NWS hourly fetch failed: {exc}")
        print(f"[!] NWS hourly fetch failed: {exc}")
        return []

    points = []
    location = f"{lat},{lon}"
    for period in periods:
        ts = period.get("startTime", datetime.now().isoformat())
        precip = (period.get("probabilityOfPrecipitation") or {}).get("value")
        for metric, value, unit in [
            ("nws_forecast_temp_f",     period.get("temperature"),   "F"),
            ("nws_forecast_wind_speed", period.get("windSpeed"),     ""),
            ("nws_forecast_wind_dir",   period.get("windDirection"), ""),
            ("nws_forecast_short",      period.get("shortForecast"), ""),
            ("nws_forecast_precip_pct", precip,                      "%"),
            ("nws_forecast_is_daytime", int(bool(period.get("isDaytime", True))), ""),
        ]:
            points.append({
                "timestamp": ts,
                "location": location,
                "metric": metric,
                "value": str(value) if value is not None else "",
                "unit": unit,
            })
    return points


def fetch_weather(config):
    api_url = config["api_url"]
    key = config["key"]
    city = config["city"]

    data_points = []

    # --- WeatherAPI current conditions ---
    try:
        resp = requests.get(api_url, params={"key": key, "q": city}, timeout=10)
        resp.raise_for_status()
        json_data = resp.json()
    except Exception as exc:
        print(f"[✖] WeatherAPI fetch failed: {exc}")
        logging.error(f"WeatherAPI fetch failed: {exc}")
        json_data = {}

    current = json_data.get("current", {})
    loc_name = json_data.get("location", {}).get("name", city)

    if current:
        ts = datetime.now().isoformat()
        data_points.extend([
            {"timestamp": ts, "location": loc_name, "metric": "temp_c",        "value": current.get("temp_c"),                        "unit": "C"},
            {"timestamp": ts, "location": loc_name, "metric": "temp_f",        "value": current.get("temp_f"),                        "unit": "F"},
            {"timestamp": ts, "location": loc_name, "metric": "humidity",      "value": current.get("humidity"),                      "unit": "%"},
            {"timestamp": ts, "location": loc_name, "metric": "wind_kph",      "value": current.get("wind_kph"),                      "unit": "kph"},
            {"timestamp": ts, "location": loc_name, "metric": "wind_dir",      "value": current.get("wind_dir"),                      "unit": ""},
            {"timestamp": ts, "location": loc_name, "metric": "condition_text","value": current.get("condition", {}).get("text"),     "unit": ""},
            {"timestamp": ts, "location": loc_name, "metric": "precip_mm",     "value": current.get("precip_mm"),                     "unit": "mm"},
            {"timestamp": ts, "location": loc_name, "metric": "uv_index",      "value": current.get("uv"),                            "unit": ""},
            {"timestamp": ts, "location": loc_name, "metric": "feels_like_c",  "value": current.get("feelslike_c"),                   "unit": "C"},
        ])

    # --- NWS alerts + hourly forecast (no API key required) ---
    lat = config.get("lat")
    lon = config.get("lon")
    if lat is not None and lon is not None:
        data_points.extend(fetch_nws_alerts(float(lat), float(lon)))
        data_points.extend(fetch_nws_hourly(float(lat), float(lon)))
    else:
        logging.info("NWS skipped: lat/lon not configured in weather stream config.")

    return data_points


def get_headers():
    return ["timestamp", "location", "metric", "value", "unit"]


def get_units():
    return ["", "", "", "", ""]
