from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def _env_int(name: str, default: int, minimum: int | None = None) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        value = default
    else:
        try:
            value = int(raw)
        except ValueError as exc:
            raise RuntimeError(f"{name} must be an integer, got {raw!r}") from exc
    if minimum is not None and value < minimum:
        raise RuntimeError(f"{name} must be >= {minimum}, got {value}")
    return value


def _env_csv(name: str, default: list[str]) -> list[str]:
    raw = os.getenv(name)
    if raw is None:
        values = list(default)
    else:
        values = [item.strip() for item in raw.split(",") if item.strip()]
        if not values:
            values = list(default)

    deduped: list[str] = []
    seen: set[str] = set()
    for item in values:
        key = item.lower()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped


def _resolve_path(path_value: str | Path, project_root: Path) -> Path:
    path = Path(path_value).expanduser()
    if not path.is_absolute():
        path = project_root / path
    return path.resolve()


def _is_project_root(path: Path) -> bool:
    return (path / "data").is_dir() and (path / "scripts").is_dir()


def discover_project_root(start: Path | None = None) -> Path:
    env_root = os.getenv("BREATHWISE_PROJECT_ROOT")
    if env_root:
        root = Path(env_root).expanduser().resolve()
        if not root.exists():
            raise RuntimeError(f"BREATHWISE_PROJECT_ROOT does not exist: {root}")
        if not _is_project_root(root):
            raise RuntimeError(f"BREATHWISE_PROJECT_ROOT is not a valid project root: {root}")
        return root

    anchor = start.resolve() if start is not None else Path(__file__).resolve()
    probe = anchor if anchor.is_dir() else anchor.parent
    for candidate in [probe, *probe.parents]:
        if _is_project_root(candidate):
            return candidate

    raise RuntimeError("Unable to discover Breathwise project root.")


@dataclass(frozen=True)
class Settings:
    project_root: Path
    marts_dir: Path
    views_dir: Path
    duckdb_path: Path
    api_auth_enabled: bool
    api_key: str | None
    api_key_header_name: str
    rate_limit_enabled: bool
    rate_limit_requests: int
    rate_limit_window_seconds: int
    cache_enabled: bool
    cache_ttl_seconds: int
    cache_max_entries: int
    api_access_log_enabled: bool
    api_access_log_path: Path
    cors_enabled: bool
    cors_allowed_origins: list[str]
    cors_allow_private_network_origins: bool
    cors_allowed_methods: list[str]
    cors_allowed_headers: list[str]
    cors_expose_headers: list[str]
    cors_allow_credentials: bool
    cors_max_age_seconds: int


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    project_root = discover_project_root(Path(__file__))

    marts_dir = _resolve_path(
        os.getenv("BREATHWISE_MARTS_DIR", str(project_root / "data" / "processed" / "marts")),
        project_root=project_root,
    )
    views_dir = _resolve_path(
        os.getenv("BREATHWISE_VIEWS_DIR", str(project_root / "data" / "processed" / "views")),
        project_root=project_root,
    )
    duckdb_path = _resolve_path(
        os.getenv("BREATHWISE_DUCKDB_PATH", str(project_root / "data" / "db" / "breathwise_tr.duckdb")),
        project_root=project_root,
    )

    api_auth_enabled = _env_bool("BREATHWISE_API_AUTH_ENABLED", default=False)
    api_key = os.getenv("BREATHWISE_API_KEY")
    api_key_header_name = os.getenv("BREATHWISE_API_KEY_HEADER_NAME", "X-API-Key").strip() or "X-API-Key"

    if api_auth_enabled and not api_key:
        raise RuntimeError(
            "BREATHWISE_API_AUTH_ENABLED=true but BREATHWISE_API_KEY is not set."
        )

    rate_limit_enabled = _env_bool("BREATHWISE_RATE_LIMIT_ENABLED", default=False)
    rate_limit_requests = _env_int("BREATHWISE_RATE_LIMIT_REQUESTS", default=240, minimum=1)
    rate_limit_window_seconds = _env_int("BREATHWISE_RATE_LIMIT_WINDOW_SECONDS", default=60, minimum=1)

    cache_enabled = _env_bool("BREATHWISE_API_CACHE_ENABLED", default=True)
    cache_ttl_seconds = _env_int("BREATHWISE_API_CACHE_TTL_SECONDS", default=30, minimum=1)
    cache_max_entries = _env_int("BREATHWISE_API_CACHE_MAX_ENTRIES", default=512, minimum=1)
    api_access_log_enabled = _env_bool("BREATHWISE_API_ACCESS_LOG_ENABLED", default=False)
    api_access_log_path = _resolve_path(
        os.getenv(
            "BREATHWISE_API_ACCESS_LOG_PATH",
            str(project_root / "data" / "processed" / "api_logs" / "api_access.jsonl"),
        ),
        project_root=project_root,
    )
    default_cors_origins = [
        "http://localhost",
        "http://127.0.0.1",
        "https://localhost",
        "https://127.0.0.1",
        "http://localhost:8081",
        "http://localhost:8082",
        "http://localhost:8083",
        "http://localhost:19006",
        "http://127.0.0.1:8081",
        "http://127.0.0.1:8082",
        "http://127.0.0.1:8083",
        "http://127.0.0.1:19006",
    ]
    cors_enabled = _env_bool("BREATHWISE_API_CORS_ENABLED", default=True)
    cors_allowed_origins = _env_csv("BREATHWISE_API_CORS_ALLOWED_ORIGINS", default=default_cors_origins)
    cors_allow_private_network_origins = _env_bool(
        "BREATHWISE_API_CORS_ALLOW_PRIVATE_NETWORK_ORIGINS", default=True
    )
    cors_allowed_methods = [
        item.upper()
        for item in _env_csv("BREATHWISE_API_CORS_ALLOWED_METHODS", default=["GET", "OPTIONS"])
    ]
    cors_allowed_headers = _env_csv(
        "BREATHWISE_API_CORS_ALLOWED_HEADERS",
        default=["Accept", "Authorization", "Content-Type", "X-Request-Id"],
    )
    if api_key_header_name.lower() not in {header.lower() for header in cors_allowed_headers}:
        cors_allowed_headers.append(api_key_header_name)
    cors_expose_headers = _env_csv(
        "BREATHWISE_API_CORS_EXPOSE_HEADERS",
        default=[
            "X-Request-Id",
            "X-Process-Time-Ms",
            "X-Breathwise-Cache",
            "X-RateLimit-Limit",
            "X-RateLimit-Remaining",
            "X-RateLimit-Window-Seconds",
            "Retry-After",
            "Content-Language",
        ],
    )
    cors_allow_credentials = _env_bool("BREATHWISE_API_CORS_ALLOW_CREDENTIALS", default=False)
    cors_max_age_seconds = _env_int("BREATHWISE_API_CORS_MAX_AGE_SECONDS", default=600, minimum=0)

    return Settings(
        project_root=project_root,
        marts_dir=marts_dir,
        views_dir=views_dir,
        duckdb_path=duckdb_path,
        api_auth_enabled=api_auth_enabled,
        api_key=api_key,
        api_key_header_name=api_key_header_name,
        rate_limit_enabled=rate_limit_enabled,
        rate_limit_requests=rate_limit_requests,
        rate_limit_window_seconds=rate_limit_window_seconds,
        cache_enabled=cache_enabled,
        cache_ttl_seconds=cache_ttl_seconds,
        cache_max_entries=cache_max_entries,
        api_access_log_enabled=api_access_log_enabled,
        api_access_log_path=api_access_log_path,
        cors_enabled=cors_enabled,
        cors_allowed_origins=cors_allowed_origins,
        cors_allow_private_network_origins=cors_allow_private_network_origins,
        cors_allowed_methods=cors_allowed_methods,
        cors_allowed_headers=cors_allowed_headers,
        cors_expose_headers=cors_expose_headers,
        cors_allow_credentials=cors_allow_credentials,
        cors_max_age_seconds=cors_max_age_seconds,
    )
