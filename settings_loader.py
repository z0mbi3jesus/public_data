import json
import os
from pathlib import Path


def load_json_config(default_name="config.json", local_name="config.local.json"):
    """Load local override config if present; otherwise load the public config file."""
    local_path = Path(local_name)
    default_path = Path(default_name)

    if local_path.exists():
        with open(local_path, encoding="utf-8") as f:
            return json.load(f)

    if default_path.exists():
        with open(default_path, encoding="utf-8") as f:
            return json.load(f)

    raise FileNotFoundError(f"Missing {local_name} and {default_name}")


def load_json_file(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def getenv_or(default, env_name):
    value = os.getenv(env_name)
    return value if value not in (None, "") else default
