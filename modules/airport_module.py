import logging
import csv
from pathlib import Path
from datetime import datetime, timezone

import requests

logging.basicConfig(
    filename='airport_debug.log',
    level=logging.DEBUG,
    format='%(asctime)s %(levelname)s %(message)s'
)


_AIRPORT_INDEX_CACHE = {}


def _get_first_non_empty(row, candidates):
    for key in candidates:
        value = row.get(key)
        if value is not None and str(value).strip() != "":
            return str(value).strip()
    return ""


def _load_airport_index(config):
    csv_path = config.get("reference_csv", "")
    if not csv_path:
        return {}

    normalized = str(Path(csv_path))
    cached = _AIRPORT_INDEX_CACHE.get(normalized)
    if cached is not None:
        return cached

    path = Path(csv_path)
    if not path.exists():
        logging.warning("OurAirports CSV not found at path: %s", csv_path)
        _AIRPORT_INDEX_CACHE[normalized] = {}
        return {}

    index = {}
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            iata = _get_first_non_empty(row, ["iata_code", "iata", "iataCode"])
            if not iata:
                continue
            code = iata.upper()
            index[code] = {
                "airport_name": _get_first_non_empty(row, ["name", "airport_name"]),
                "municipality": _get_first_non_empty(row, ["municipality", "city"]),
                "iso_region": _get_first_non_empty(row, ["iso_region", "region"]),
                "country": _get_first_non_empty(row, ["iso_country", "country"]),
                "latitude_deg": _get_first_non_empty(row, ["latitude_deg", "latitude"]),
                "longitude_deg": _get_first_non_empty(row, ["longitude_deg", "longitude"]),
                "elevation_ft": _get_first_non_empty(row, ["elevation_ft", "elevation"]),
                "airport_type": _get_first_non_empty(row, ["type", "airport_type"])
            }

    _AIRPORT_INDEX_CACHE[normalized] = index
    logging.info("Loaded %d airport records from %s", len(index), csv_path)
    return index


def fetch_airport(config):
    """Fetch airport/flight data and return normalized records."""
    api_url = config["api_url"]
    key = config.get("key", "")
    airports = config.get("airports", [])
    airport_param = config.get("airport_param", "dep_iata")
    key_param = config.get("key_param", "access_key")
    base_params = config.get("params", {})
    timeout_sec = int(config.get("timeout_sec", 20))
    airport_index = _load_airport_index(config)

    data_points = []

    for airport in airports:
        airport_upper = str(airport).upper()
        params = dict(base_params)
        if key:
            params[key_param] = key
        params[airport_param] = airport_upper

        try:
            resp = requests.get(api_url, params=params, timeout=timeout_sec)
            resp.raise_for_status()
            payload = resp.json()
        except Exception as e:
            print(f"[FAIL] Airport fetch failed for {airport_upper}: {e}")
            logging.error("Airport fetch failed for %s: %s", airport_upper, e)
            continue

        records = payload.get("data", []) if isinstance(payload, dict) else payload
        if not isinstance(records, list):
            logging.warning("Unexpected airport payload type for %s: %s", airport_upper, type(payload))
            continue

        for rec in records:
            flight = rec.get("flight", {}) if isinstance(rec, dict) else {}
            airline = rec.get("airline", {}) if isinstance(rec, dict) else {}
            departure = rec.get("departure", {}) if isinstance(rec, dict) else {}
            arrival = rec.get("arrival", {}) if isinstance(rec, dict) else {}
            reference = airport_index.get(airport_upper, {})

            data_points.append({
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "airport_code": airport_upper,
                "flight_iata": flight.get("iata") or rec.get("flight_iata"),
                "airline": airline.get("name") or rec.get("airline_name"),
                "status": rec.get("flight_status") or rec.get("status"),
                "dep_scheduled": departure.get("scheduled") or rec.get("departure_scheduled"),
                "arr_scheduled": arrival.get("scheduled") or rec.get("arrival_scheduled"),
                "dep_delay_min": departure.get("delay") or rec.get("departure_delay"),
                "arr_delay_min": arrival.get("delay") or rec.get("arrival_delay"),
                "airport_name": reference.get("airport_name", ""),
                "municipality": reference.get("municipality", ""),
                "iso_region": reference.get("iso_region", ""),
                "country": reference.get("country", ""),
                "latitude_deg": reference.get("latitude_deg", ""),
                "longitude_deg": reference.get("longitude_deg", ""),
                "elevation_ft": reference.get("elevation_ft", ""),
                "airport_type": reference.get("airport_type", ""),
                "unit": "minutes"
            })

    return data_points


def get_headers():
    return [
        "timestamp",
        "airport_code",
        "flight_iata",
        "airline",
        "status",
        "dep_scheduled",
        "arr_scheduled",
        "dep_delay_min",
        "arr_delay_min",
        "airport_name",
        "municipality",
        "iso_region",
        "country",
        "latitude_deg",
        "longitude_deg",
        "elevation_ft",
        "airport_type",
        "unit"
    ]


def get_units():
    return ["", "", "", "", "", "", "", "minutes", "minutes", "", "", "", "", "deg", "deg", "ft", "", ""]
