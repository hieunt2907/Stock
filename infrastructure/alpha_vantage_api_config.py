from __future__ import annotations

import os
import time
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[1]
ENV_PATH = PROJECT_ROOT / ".env"


@dataclass(frozen=True)
class AlphaVantageApiConfig:
    api_key: str | None
    base_url: str
    rate_limit_batch_size: int
    rate_limit_cooldown_seconds: float


class AlphaVantageRateLimiter:
    def __init__(self, config: AlphaVantageApiConfig) -> None:
        self.config = config
        self._request_count = 0

    def wait(self) -> None:
        if (
            self.config.rate_limit_batch_size > 0
            and self._request_count > 0
            and self._request_count % self.config.rate_limit_batch_size == 0
        ):
            time.sleep(self.config.rate_limit_cooldown_seconds)

    def mark_request(self) -> None:
        self._request_count += 1


def _get_float_env(name: str, default: float) -> float:
    raw_value = os.getenv(name)
    if raw_value is None:
        return default

    try:
        return float(raw_value)
    except ValueError:
        return default


def _get_int_env(name: str, default: int) -> int:
    raw_value = os.getenv(name)
    if raw_value is None:
        return default

    try:
        return int(raw_value)
    except ValueError:
        return default


def get_alpha_vantage_api_config() -> AlphaVantageApiConfig:
    load_dotenv(ENV_PATH)
    return AlphaVantageApiConfig(
        api_key=os.getenv("ALPHA_VANTAGE_API_KEY") or None,
        base_url=os.getenv("ALPHA_VANTAGE_BASE_URL", "https://www.alphavantage.co/query"),
        rate_limit_batch_size=_get_int_env("ALPHA_VANTAGE_RATE_LIMIT_BATCH_SIZE", 30),
        rate_limit_cooldown_seconds=_get_float_env("ALPHA_VANTAGE_RATE_LIMIT_COOLDOWN_SECONDS", 20.0),
    )


alpha_vantage_api_config = get_alpha_vantage_api_config()
alpha_vantage_rate_limiter = AlphaVantageRateLimiter(alpha_vantage_api_config)
