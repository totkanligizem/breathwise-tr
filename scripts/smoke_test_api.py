from __future__ import annotations

import os
from dataclasses import dataclass

from fastapi.testclient import TestClient

from backend.main import app


@dataclass(frozen=True)
class Check:
    path: str
    min_len: int | None = None


def auth_headers() -> dict[str, str]:
    if os.getenv("BREATHWISE_API_AUTH_ENABLED", "").strip().lower() not in {"1", "true", "yes", "on"}:
        return {}

    api_key = os.getenv("BREATHWISE_API_KEY")
    if not api_key:
        raise RuntimeError("BREATHWISE_API_AUTH_ENABLED=true but BREATHWISE_API_KEY is not set")

    header_name = os.getenv("BREATHWISE_API_KEY_HEADER_NAME", "X-API-Key").strip() or "X-API-Key"
    return {header_name: api_key}


def main() -> None:
    checks = [
        Check("/health"),
        Check("/ready"),
        Check("/v1/meta/ops-status"),
        Check("/v1/meta/ops-status?compact=true"),
        Check("/v1/meta/localization"),
        Check("/v1/meta/localization?include_translations=true&max_keys=5"),
        Check("/v1/meta/datasets", min_len=1),
        Check("/v1/cities/current?limit=3&locale=tr-TR", min_len=1),
        Check("/v1/provinces/map-metrics?limit=3&locale=en-US", min_len=1),
        Check("/v1/mobile/cities/current?limit=3&locale=en-US", min_len=1),
        Check("/v1/mobile/provinces/map-metrics?limit=3&locale=tr", min_len=1),
        Check("/v1/cities/Ankara/hourly?limit=3", min_len=1),
        Check("/v1/mobile/cities/Ankara/timeline?limit=3", min_len=1),
    ]

    headers = auth_headers()

    client = TestClient(app)
    failures: list[str] = []

    for check in checks:
        response = client.get(check.path, headers=headers)
        if response.status_code != 200:
            failures.append(f"{check.path} -> status={response.status_code}")
            continue

        if check.min_len is not None:
            payload = response.json()
            if not isinstance(payload, list) or len(payload) < check.min_len:
                failures.append(f"{check.path} -> unexpected payload length")

        print(f"OK {check.path}")

    if failures:
        print("\nSmoke checks failed:")
        for failure in failures:
            print(f"- {failure}")
        raise SystemExit(1)

    print("\nAll API smoke checks passed.")


if __name__ == "__main__":
    main()
