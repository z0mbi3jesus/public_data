import requests
from datetime import datetime


def fetch_purpleair(config):
    """
    Fetch PurpleAir sensor data for a map bounding box.
    """
    api_url = config.get("api_url", "https://api.purpleair.com/v1/sensors")
    key = config.get("key", "")
    timeout_sec = int(config.get("timeout_sec", 15))

    headers = {}
    if key:
        headers["X-API-Key"] = key

    params = {
        "fields": "name,latitude,longitude,pm2.5,humidity,temperature,confidence",
    }

    bbox = config.get("bbox", {})
    if all(k in bbox for k in ("nwlat", "nwlng", "selat", "selng")):
        params.update(
            {
                "nwlat": bbox["nwlat"],
                "nwlng": bbox["nwlng"],
                "selat": bbox["selat"],
                "selng": bbox["selng"],
            }
        )

    try:
        resp = requests.get(api_url, headers=headers, params=params, timeout=timeout_sec)
        resp.raise_for_status()
        json_data = resp.json()
    except Exception as exc:
        print(f"[✖] PurpleAir fetch failed: {exc}")
        return []

    fields = json_data.get("fields") or []
    data = json_data.get("data") or []

    idx = {name: i for i, name in enumerate(fields)}
    ts = datetime.now().isoformat()
    data_points = []

    for row in data:
        lat = row[idx["latitude"]] if "latitude" in idx else None
        lon = row[idx["longitude"]] if "longitude" in idx else None
        location = f"{lat},{lon}" if lat is not None and lon is not None else "unknown"

        metrics = [
            ("purpleair_pm25", "pm2.5", "ug/m3"),
            ("purpleair_humidity", "humidity", "%"),
            ("purpleair_temp_f", "temperature", "F"),
            ("purpleair_confidence", "confidence", "%"),
            ("purpleair_sensor_name", "name", ""),
        ]

        for metric_name, source_key, unit in metrics:
            value = row[idx[source_key]] if source_key in idx else None
            data_points.append(
                {
                    "timestamp": ts,
                    "location": location,
                    "metric": metric_name,
                    "value": value,
                    "unit": unit,
                }
            )

    return data_points


def get_headers():
    return ["timestamp", "location", "metric", "value", "unit"]


def get_units():
    return ["", "", "", "", ""]
