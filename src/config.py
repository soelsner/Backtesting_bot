"""Configuration loading for the backtesting framework."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

import yaml


@dataclass(frozen=True)
class MassiveConfig:
    api_key: str
    base_url: str = "https://api.polygon.io"


@dataclass(frozen=True)
class AlpacaConfig:
    api_key: str
    secret_key: str
    base_url: str = "https://paper-api.alpaca.markets"
    data_base_url: str = "https://data.alpaca.markets"


@dataclass(frozen=True)
class LocalPaths:
    data_dir: Path


@dataclass(frozen=True)
class AppConfig:
    massive: Optional[MassiveConfig]
    alpaca: AlpacaConfig
    local: LocalPaths


DEFAULT_CONFIG_PATH = Path("config.yaml")
DEFAULT_ENV_PATH = Path(".env")


class ConfigError(RuntimeError):
    """Raised when required configuration is missing."""


def _load_yaml(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    return data


def _get_env(name: str) -> Optional[str]:
    value = os.getenv(name)
    return value.strip() if value else None


def _load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def load_config(path: Optional[Path] = None) -> AppConfig:
    _load_dotenv(DEFAULT_ENV_PATH)
    config_path = path or DEFAULT_CONFIG_PATH
    raw = _load_yaml(config_path)

    massive_cfg = raw.get("massive", {})
    alpaca_cfg = raw.get("alpaca", {})
    local_cfg = raw.get("local", {})

    massive_key = _get_env("MASSIVE_API_KEY") or massive_cfg.get("api_key")
    alpaca_key = _get_env("ALPACA_API_KEY") or alpaca_cfg.get("api_key")
    alpaca_secret = _get_env("ALPACA_SECRET_KEY") or alpaca_cfg.get("secret_key")

    if not alpaca_key:
        raise ConfigError("Missing ALPACA_API_KEY (env or config).")
    if not alpaca_secret:
        raise ConfigError("Missing ALPACA_SECRET_KEY (env or config).")

    massive_base = massive_cfg.get("base_url") or "https://api.polygon.io"
    alpaca_base = alpaca_cfg.get("base_url") or "https://paper-api.alpaca.markets"
    alpaca_data_base = alpaca_cfg.get("data_base_url") or "https://data.alpaca.markets"
    data_dir = Path(local_cfg.get("data_dir", "data_local"))

    return AppConfig(
        massive=MassiveConfig(api_key=massive_key, base_url=massive_base)
        if massive_key
        else None,
        alpaca=AlpacaConfig(
            api_key=alpaca_key,
            secret_key=alpaca_secret,
            base_url=alpaca_base,
            data_base_url=alpaca_data_base,
        ),
        local=LocalPaths(data_dir=data_dir),
    )
