import logging
logging.basicConfig(
    filename='traffic_debug.log',
    level=logging.DEBUG,
    format='%(asctime)s %(levelname)s %(message)s'
)
logging.debug('traffic_module loaded')
import requests
from datetime import datetime


def _fetch_road_incidents(inc_cfg):
    """
    Fetch optional 511 / DOT incident feed and emit flat metric rows.
    Expects GeoJSON FeatureCollection or list of features.
    """
    if not inc_cfg or not inc_cfg.get("enabled"):
        return []

    api_url = inc_cfg.get("api_url", "")
    if not api_url:
        return []

    timeout_sec = int(inc_cfg.get("timeout_sec", 20))
    params = inc_cfg.get("params", {})
    headers = {}

    key = inc_cfg.get("key")
    if key:
        headers[inc_cfg.get("key_header", "x-api-key")] = key

    try:
        resp = requests.get(api_url, params=params, headers=headers, timeout=timeout_sec)
        resp.raise_for_status()
        json_data = resp.json()
    except Exception as exc:
        print(f"[✖] Road incidents fetch failed: {exc}")
        return []

    features = json_data.get("features") if isinstance(json_data, dict) else None
    if features is None and isinstance(json_data, list):
        features = json_data
    if not isinstance(features, list):
        return []

    now = datetime.now().isoformat()
    points = []
    for feature in features:
        props = feature.get("properties", {}) if isinstance(feature, dict) else {}
        geom = feature.get("geometry", {}) if isinstance(feature, dict) else {}
        coords = geom.get("coordinates", []) if isinstance(geom, dict) else []

        location = "unknown"
        if isinstance(coords, list) and len(coords) >= 2:
            location = f"{coords[1]},{coords[0]}"

        for metric, value, unit in [
            ("incident_type", props.get("type") or props.get("event_type") or props.get("category"), ""),
            ("incident_severity", props.get("severity") or props.get("impact"), ""),
            ("incident_status", props.get("status"), ""),
            ("incident_start", props.get("start_time") or props.get("start"), ""),
            ("incident_end", props.get("end_time") or props.get("end"), ""),
            ("incident_lanes_blocked", props.get("lanes_blocked"), "count"),
            ("incident_description", props.get("description") or props.get("headline"), ""),
        ]:
            points.append({
                "timestamp": now,
                "location": location,
                "metric": metric,
                "value": value,
                "unit": unit,
            })

    return points


def fetch_traffic(config):
    api_url = config["api_url"]
    key = config["key"]
    points = config.get("points", [])

    data_points = []

    for pt in points:
        params = {"key": key, "point": pt["coords"]}
        try:
            resp = requests.get(api_url, params=params)
            resp.raise_for_status()
            json_data = resp.json()
            fsd = json_data.get("flowSegmentData", {})
            logging.debug(f"Fetched data for {pt['label']}: {fsd}")
        except Exception as e:
            print(f"[✖] Traffic fetch failed for {pt['label']}: {e}")
            continue

        metrics = {
            "currentSpeed": "kph",
            "freeFlowSpeed": "kph",
            "currentTravelTime": "sec",
            "freeFlowTravelTime": "sec",
            "confidence": ""
        }

        for metric, unit in metrics.items():
            data_points.append({
                "timestamp": datetime.now().isoformat(),
                "location": pt["label"],
                "metric": metric,
                "value": fsd.get(metric),
                "unit": unit
            })

    # Optional 511/DOT incident feed nested under traffic config.
    data_points.extend(_fetch_road_incidents(config.get("incidents", {})))

    return data_points

def get_headers():
    return ["timestamp", "location", "metric", "value", "unit"]

def get_units():
    return ["", "", "", "", ""]
