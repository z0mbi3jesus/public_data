import json
import os
from pathlib import Path


def _apply_env_overrides(cfg: dict) -> dict:
    """
    Apply environment variable overrides on top of loaded config.
    Any PD_* env var takes precedence over file values.

    Variables:
        PD_MYSQL_HOST, PD_MYSQL_PORT, PD_MYSQL_USER, PD_MYSQL_PASSWORD, PD_MYSQL_DATABASE
        PD_PROVIDER_KEY, PD_PROVIDER_OWNER_NAME
        PD_ADMIN_EMAIL, PD_ADMIN_PASSWORD
        PD_AIRNOW_KEY, PD_WEATHER_KEY, PD_TOMTOM_KEY, PD_AVIATION_KEY
        PD_SECURE_COOKIES  (set to "true" when running behind HTTPS)
    """

    def _set(d: dict, key: str, env_name: str):
        v = os.getenv(env_name)
        if v is not None:
            d[key] = v

    mysql = cfg.setdefault("storage", {}).setdefault("mysql", {})
    _set(mysql, "host", "PD_MYSQL_HOST")
    _set(mysql, "port", "PD_MYSQL_PORT")
    _set(mysql, "user", "PD_MYSQL_USER")
    _set(mysql, "password", "PD_MYSQL_PASSWORD")
    _set(mysql, "database", "PD_MYSQL_DATABASE")

    provider = cfg.setdefault("provider", {})
    _set(provider, "key", "PD_PROVIDER_KEY")
    _set(provider, "owner_name", "PD_PROVIDER_OWNER_NAME")

    admin = cfg.setdefault("admin", {})
    _set(admin, "bootstrap_email", "PD_ADMIN_EMAIL")
    _set(admin, "bootstrap_password", "PD_ADMIN_PASSWORD")

    streams = cfg.setdefault("streams", {})
    _set(streams.setdefault("air_quality", {}), "key", "PD_AIRNOW_KEY")
    _set(streams.setdefault("weather", {}), "key", "PD_WEATHER_KEY")
    _set(streams.setdefault("traffic", {}), "key", "PD_TOMTOM_KEY")
    _set(streams.setdefault("airport", {}), "key", "PD_AVIATION_KEY")

    web = cfg.setdefault("web", {})
    sc = os.getenv("PD_SECURE_COOKIES")
    if sc is not None:
        web["secure_cookies"] = sc.lower() in ("1", "true", "yes")

    return cfg


def load_json_config(default_name="config.json", local_name="config.local.json") -> dict:
    """
    Load config from the local override file if present, otherwise the public file.
    Environment variables (PD_*) are applied on top and always win.
    """
    local_path = Path(local_name)
    default_path = Path(default_name)

    if local_path.exists():
        with open(local_path, encoding="utf-8") as f:
            cfg = json.load(f)
    elif default_path.exists():
        with open(default_path, encoding="utf-8") as f:
            cfg = json.load(f)
    else:
        raise FileNotFoundError(f"Missing {local_name} and {default_name}")

    return _apply_env_overrides(cfg)


def load_json_file(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def getenv_or(default, env_name):
    value = os.getenv(env_name)
    return value if value not in (None, "") else default
