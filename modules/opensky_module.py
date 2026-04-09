import requests
from datetime import datetime, timedelta


# ── OAuth2 token manager ──────────────────────────────────────────────────────

_TOKEN_URL = (
    "https://auth.opensky-network.org/auth/realms/opensky-network"
    "/protocol/openid-connect/token"
)
_TOKEN_REFRESH_MARGIN_SEC = 60  # refresh this many seconds before expiry


class _TokenManager:
    def __init__(self, client_id, client_secret):
        self.client_id = client_id
        self.client_secret = client_secret
        self._token = None
        self._expires_at = None

    def get_token(self):
        if self._token and self._expires_at and datetime.now() < self._expires_at:
            return self._token
        return self._refresh()

    def _refresh(self):
        resp = requests.post(
            _TOKEN_URL,
            data={
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
            },
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
        self._token = data["access_token"]
        expires_in = data.get("expires_in", 1800)
        self._expires_at = datetime.now() + timedelta(
            seconds=expires_in - _TOKEN_REFRESH_MARGIN_SEC
        )
        return self._token

    def headers(self):
        return {"Authorization": f"Bearer {self.get_token()}"}


# One shared instance per process; re-created if credentials change.
_token_manager: _TokenManager | None = None
_token_manager_key: tuple | None = None


def _get_auth_headers(config):
    """Return Bearer auth headers if client_id/secret are configured, else {}."""
    global _token_manager, _token_manager_key
    client_id = config.get("client_id", "")
    client_secret = config.get("client_secret", "")
    if not client_id or not client_secret:
        return {}
    key = (client_id, client_secret)
    if _token_manager is None or _token_manager_key != key:
        _token_manager = _TokenManager(client_id, client_secret)
        _token_manager_key = key
    try:
        return _token_manager.headers()
    except Exception as exc:
        print(f"[!] OpenSky token refresh failed: {exc}")
        return {}


# ── State vector field positions (per OpenSky docs) ──────────────────────────
# 0  icao24          1  callsign        2  origin_country
# 3  time_position   4  last_contact    5  longitude
# 6  latitude        7  baro_altitude   8  on_ground
# 9  velocity        10 true_track      11 vertical_rate
# 12 sensors         13 geo_altitude    14 squawk
# 15 spi             16 position_source 17 category (only if extended=1)


def _sv(state, idx, default=None):
    return state[idx] if len(state) > idx else default


def fetch_opensky(config):
    """
    Fetch live aircraft state vectors from OpenSky and flatten to stream rows.
    Supports OAuth2 client credentials (client_id + client_secret in config)
    and falls back to anonymous mode when credentials are absent.
    """
    base = config.get("api_url", "https://opensky-network.org/api").rstrip("/")
    lamin = config.get("lamin")
    lomin = config.get("lomin")
    lamax = config.get("lamax")
    lomax = config.get("lomax")
    timeout_sec = int(config.get("timeout_sec", 15))

    params = {}
    if all(v is not None for v in (lamin, lomin, lamax, lomax)):
        params = {"lamin": lamin, "lomin": lomin, "lamax": lamax, "lomax": lomax}

    auth_headers = _get_auth_headers(config)
    authed = bool(auth_headers)

    try:
        resp = requests.get(
            f"{base}/states/all",
            params=params,
            headers=auth_headers,
            timeout=timeout_sec,
        )
        if resp.status_code == 401 and authed:
            # Token may have expired mid-flight; clear cache and retry once.
            global _token_manager
            _token_manager = None
            auth_headers = _get_auth_headers(config)
            resp = requests.get(
                f"{base}/states/all",
                params=params,
                headers=auth_headers,
                timeout=timeout_sec,
            )
        resp.raise_for_status()
        json_data = resp.json()
    except Exception as exc:
        print(f"[✖] OpenSky fetch failed: {exc}")
        return []

    states = json_data.get("states") or []
    ts = datetime.now().isoformat()
    data_points = []

    for state in states:
        icao24    = _sv(state, 0, "")
        callsign  = (_sv(state, 1) or "").strip()
        country   = _sv(state, 2, "")
        lon       = _sv(state, 5)
        lat       = _sv(state, 6)
        alt_baro  = _sv(state, 7)
        on_ground = int(bool(_sv(state, 8, False)))
        velocity  = _sv(state, 9)
        true_track    = _sv(state, 10)
        vertical_rate = _sv(state, 11)
        alt_geo   = _sv(state, 13)
        squawk    = _sv(state, 14, "")
        pos_src   = _sv(state, 16)  # 0=ADS-B 1=ASTERIX 2=MLAT 3=FLARM

        location = f"{lat},{lon}" if lat is not None and lon is not None else "unknown"

        for metric, value, unit in [
            ("opensky_aircraft_count",  1,             "count"),
            ("opensky_on_ground",       on_ground,     "bool"),
            ("opensky_velocity_ms",     velocity,      "m/s"),
            ("opensky_altitude_baro_m", alt_baro,      "m"),
            ("opensky_altitude_geo_m",  alt_geo,       "m"),
            ("opensky_true_track_deg",  true_track,    "deg"),
            ("opensky_vertical_rate_ms",vertical_rate, "m/s"),
            ("opensky_squawk",          squawk,        ""),
            ("opensky_pos_source",      pos_src,       ""),
            ("opensky_callsign",        callsign,      ""),
            ("opensky_country",         country,       ""),
            ("opensky_icao24",          icao24,        ""),
        ]:
            data_points.append(
                {
                    "timestamp": ts,
                    "location":  location,
                    "metric":    metric,
                    "value":     value,
                    "unit":      unit,
                }
            )

    mode = "authenticated" if authed else "anonymous"
    print(f"[OK] OpenSky: {len(states)} aircraft ({mode})")
    return data_points


def get_headers():
    return ["timestamp", "location", "metric", "value", "unit"]


def get_units():
    return ["", "", "", "", ""]
