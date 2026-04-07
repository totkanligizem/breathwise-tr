from __future__ import annotations

import importlib
import re

from fastapi.testclient import TestClient


def load_app_with_env(monkeypatch, **env: str) -> object:
    # Keep tests deterministic by resetting relevant env vars explicitly.
    keys = [
        "BREATHWISE_API_AUTH_ENABLED",
        "BREATHWISE_API_KEY",
        "BREATHWISE_API_KEY_HEADER_NAME",
        "BREATHWISE_API_CORS_ENABLED",
        "BREATHWISE_API_CORS_ALLOWED_ORIGINS",
        "BREATHWISE_API_CORS_ALLOW_PRIVATE_NETWORK_ORIGINS",
        "BREATHWISE_API_CORS_ALLOWED_METHODS",
        "BREATHWISE_API_CORS_ALLOWED_HEADERS",
        "BREATHWISE_API_CORS_EXPOSE_HEADERS",
        "BREATHWISE_API_CORS_ALLOW_CREDENTIALS",
        "BREATHWISE_API_CORS_MAX_AGE_SECONDS",
        "BREATHWISE_RATE_LIMIT_ENABLED",
        "BREATHWISE_RATE_LIMIT_REQUESTS",
        "BREATHWISE_RATE_LIMIT_WINDOW_SECONDS",
        "BREATHWISE_API_CACHE_ENABLED",
        "BREATHWISE_API_CACHE_TTL_SECONDS",
        "BREATHWISE_API_CACHE_MAX_ENTRIES",
        "BREATHWISE_API_ACCESS_LOG_ENABLED",
        "BREATHWISE_API_ACCESS_LOG_PATH",
    ]

    for key in keys:
        if key in env:
            monkeypatch.setenv(key, env[key])
        else:
            monkeypatch.delenv(key, raising=False)

    import backend.config as config_module
    import backend.main as main_module

    importlib.reload(config_module)
    importlib.reload(main_module)
    config_module.get_settings.cache_clear()
    main_module.reset_runtime_state()

    return main_module


def _is_absolute_path_like(value: str) -> bool:
    return value.startswith("/") or value.startswith("\\\\") or bool(re.match(r"^[A-Za-z]:[\\\\/]", value))


def assert_no_absolute_path_leaks(payload: object) -> None:
    path_keys = {"path", "project_root"}
    if isinstance(payload, dict):
        for key, value in payload.items():
            is_path_key = key in path_keys or key.endswith("_path") or key.endswith("_dir") or key.endswith("_manifest")
            if is_path_key and isinstance(value, str):
                assert not _is_absolute_path_like(value), f"Unexpected absolute path in {key}: {value}"
            assert_no_absolute_path_leaks(value)
    elif isinstance(payload, list):
        for item in payload:
            assert_no_absolute_path_leaks(item)


def test_ready_and_invalid_time_range(monkeypatch) -> None:
    main_module = load_app_with_env(
        monkeypatch,
        BREATHWISE_API_AUTH_ENABLED="false",
        BREATHWISE_RATE_LIMIT_ENABLED="false",
        BREATHWISE_API_CACHE_ENABLED="false",
    )

    client = TestClient(main_module.app)

    ready = client.get("/ready")
    assert ready.status_code == 200
    assert ready.json()["ready"] is True
    assert ready.headers.get("X-Process-Time-Ms") is not None

    ops_status = client.get("/v1/meta/ops-status")
    assert ops_status.status_code == 200
    ops_payload = ops_status.json()
    assert "latest_run" in ops_payload
    assert "validation" in ops_payload
    assert "scheduler_health" in ops_payload
    assert "summary" in ops_payload
    assert "alerting" in ops_payload

    compact = client.get("/v1/meta/ops-status", params={"compact": "true"})
    assert compact.status_code == 200
    compact_payload = compact.json()
    assert "scheduler_health" in compact_payload
    assert "modes" not in compact_payload["scheduler_health"]
    assert "alerting" in compact_payload

    localization = client.get("/v1/meta/localization")
    assert localization.status_code == 200
    localization_payload = localization.json()
    assert "supported_locales" in localization_payload
    assert "translation_key_count" in localization_payload
    assert localization.headers.get("Content-Language") in {"tr-TR", "en-US"}

    localization_with_keys = client.get(
        "/v1/meta/localization",
        params={"include_translations": "true", "max_keys": "5"},
    )
    assert localization_with_keys.status_code == 200
    localization_with_keys_payload = localization_with_keys.json()
    assert "translation_keys" in localization_with_keys_payload
    assert localization_with_keys_payload["translation_keys_returned"] <= 5

    city_current_en = client.get("/v1/cities/current", params={"limit": 3, "locale": "en-US"})
    assert city_current_en.status_code == 200
    city_payload = city_current_en.json()
    assert isinstance(city_payload, list)
    assert len(city_payload) >= 1
    assert "aq_category_key" in city_payload[0]
    assert "aq_category_label" in city_payload[0]
    assert city_current_en.headers.get("Content-Language") == "en-US"

    province_tr = client.get("/v1/provinces/map-metrics", params={"limit": 3, "locale": "tr"})
    assert province_tr.status_code == 200
    province_payload = province_tr.json()
    assert isinstance(province_payload, list)
    assert len(province_payload) >= 1
    assert "aq_alert_key" in province_payload[0]
    assert "heat_alert_key" in province_payload[0]
    assert province_tr.headers.get("Content-Language") == "tr-TR"

    invalid_locale = client.get("/v1/cities/current", params={"locale": "de-DE"})
    assert invalid_locale.status_code == 422
    assert invalid_locale.json()["error"]["code"] == "http_error"

    bad_range = client.get(
        "/v1/cities/Ankara/hourly",
        params={"start": "2024-01-02T00:00", "end": "2024-01-01T00:00"},
    )
    assert bad_range.status_code == 422
    payload = bad_range.json()
    assert payload["error"]["code"] == "http_error"


def test_publish_safe_mode_redacts_absolute_paths(monkeypatch) -> None:
    main_module = load_app_with_env(
        monkeypatch,
        BREATHWISE_API_AUTH_ENABLED="false",
        BREATHWISE_RATE_LIMIT_ENABLED="false",
        BREATHWISE_API_CACHE_ENABLED="false",
    )

    client = TestClient(main_module.app)

    health = client.get("/health", params={"publish_safe": "true"})
    assert health.status_code == 200
    assert_no_absolute_path_leaks(health.json())

    datasets = client.get("/v1/meta/datasets", params={"publish_safe": "true"})
    assert datasets.status_code == 200
    assert_no_absolute_path_leaks(datasets.json())

    localization = client.get("/v1/meta/localization", params={"publish_safe": "true"})
    assert localization.status_code == 200
    assert_no_absolute_path_leaks(localization.json())

    ops = client.get("/v1/meta/ops-status", params={"publish_safe": "true"})
    assert ops.status_code == 200
    assert_no_absolute_path_leaks(ops.json())


def test_api_key_auth_enforced(monkeypatch) -> None:
    main_module = load_app_with_env(
        monkeypatch,
        BREATHWISE_API_AUTH_ENABLED="true",
        BREATHWISE_API_KEY="unit-test-key",
        BREATHWISE_API_KEY_HEADER_NAME="X-API-Key",
        BREATHWISE_RATE_LIMIT_ENABLED="false",
        BREATHWISE_API_CACHE_ENABLED="false",
    )

    client = TestClient(main_module.app)

    unauthorized = client.get("/v1/meta/datasets")
    assert unauthorized.status_code == 401
    unauthorized_ops = client.get("/v1/meta/ops-status")
    assert unauthorized_ops.status_code == 401
    unauthorized_localization = client.get("/v1/meta/localization")
    assert unauthorized_localization.status_code == 401
    unauthorized_ops_compact = client.get("/v1/meta/ops-status", params={"compact": "true"})
    assert unauthorized_ops_compact.status_code == 401

    authorized = client.get("/v1/meta/datasets", headers={"X-API-Key": "unit-test-key"})
    assert authorized.status_code == 200
    authorized_ops = client.get("/v1/meta/ops-status", headers={"X-API-Key": "unit-test-key"})
    assert authorized_ops.status_code == 200
    authorized_localization = client.get("/v1/meta/localization", headers={"X-API-Key": "unit-test-key"})
    assert authorized_localization.status_code == 200


def test_cors_preflight_allows_web_origin_and_auth_headers(monkeypatch) -> None:
    main_module = load_app_with_env(
        monkeypatch,
        BREATHWISE_API_AUTH_ENABLED="true",
        BREATHWISE_API_KEY="unit-test-key",
        BREATHWISE_API_KEY_HEADER_NAME="X-API-Key",
        BREATHWISE_API_CORS_ENABLED="true",
        BREATHWISE_API_CORS_ALLOWED_ORIGINS="http://localhost:8083",
        BREATHWISE_API_CORS_ALLOW_PRIVATE_NETWORK_ORIGINS="false",
        BREATHWISE_API_CORS_ALLOWED_METHODS="GET,OPTIONS",
        BREATHWISE_API_CORS_ALLOWED_HEADERS="Authorization,Content-Type,X-API-Key",
        BREATHWISE_RATE_LIMIT_ENABLED="false",
        BREATHWISE_API_CACHE_ENABLED="false",
    )
    client = TestClient(main_module.app)

    response = client.options(
        "/v1/meta/datasets",
        headers={
            "Origin": "http://localhost:8083",
            "Access-Control-Request-Method": "GET",
            "Access-Control-Request-Headers": "x-api-key,authorization,content-type",
        },
    )

    assert response.status_code == 200
    assert response.headers.get("access-control-allow-origin") == "http://localhost:8083"
    allow_headers = (response.headers.get("access-control-allow-headers") or "").lower()
    assert "x-api-key" in allow_headers
    assert "authorization" in allow_headers


def test_cors_preflight_disallowed_origin_returns_400(monkeypatch) -> None:
    main_module = load_app_with_env(
        monkeypatch,
        BREATHWISE_API_AUTH_ENABLED="false",
        BREATHWISE_API_CORS_ENABLED="true",
        BREATHWISE_API_CORS_ALLOWED_ORIGINS="http://localhost:8083",
        BREATHWISE_API_CORS_ALLOW_PRIVATE_NETWORK_ORIGINS="false",
        BREATHWISE_RATE_LIMIT_ENABLED="false",
        BREATHWISE_API_CACHE_ENABLED="false",
    )
    client = TestClient(main_module.app)

    response = client.options(
        "/v1/mobile/cities/current",
        headers={
            "Origin": "http://localhost:8089",
            "Access-Control-Request-Method": "GET",
            "Access-Control-Request-Headers": "x-api-key",
        },
    )

    assert response.status_code == 400


def test_cors_headers_on_authorized_get(monkeypatch) -> None:
    main_module = load_app_with_env(
        monkeypatch,
        BREATHWISE_API_AUTH_ENABLED="true",
        BREATHWISE_API_KEY="unit-test-key",
        BREATHWISE_API_KEY_HEADER_NAME="X-API-Key",
        BREATHWISE_API_CORS_ENABLED="true",
        BREATHWISE_API_CORS_ALLOWED_ORIGINS="http://localhost:8083",
        BREATHWISE_API_CORS_ALLOW_PRIVATE_NETWORK_ORIGINS="false",
        BREATHWISE_RATE_LIMIT_ENABLED="false",
        BREATHWISE_API_CACHE_ENABLED="false",
    )
    client = TestClient(main_module.app)

    response = client.get(
        "/v1/meta/datasets",
        headers={
            "Origin": "http://localhost:8083",
            "X-API-Key": "unit-test-key",
        },
    )

    assert response.status_code == 200
    assert response.headers.get("access-control-allow-origin") == "http://localhost:8083"


def test_rate_limit_returns_429(monkeypatch) -> None:
    main_module = load_app_with_env(
        monkeypatch,
        BREATHWISE_API_AUTH_ENABLED="false",
        BREATHWISE_RATE_LIMIT_ENABLED="true",
        BREATHWISE_RATE_LIMIT_REQUESTS="2",
        BREATHWISE_RATE_LIMIT_WINDOW_SECONDS="60",
        BREATHWISE_API_CACHE_ENABLED="false",
    )

    client = TestClient(main_module.app)

    r1 = client.get("/v1/mobile/provinces/map-metrics", params={"limit": 1})
    r2 = client.get("/v1/mobile/provinces/map-metrics", params={"limit": 1})
    r3 = client.get("/v1/mobile/provinces/map-metrics", params={"limit": 1})

    assert r1.status_code == 200
    assert r2.status_code == 200
    assert r3.status_code == 429
    assert r2.headers.get("X-RateLimit-Limit") == "2"
    assert r3.headers.get("X-RateLimit-Limit") == "2"
    assert r3.json()["error"]["code"] == "rate_limit_exceeded"


def test_cache_header_hit_after_first_request(monkeypatch) -> None:
    main_module = load_app_with_env(
        monkeypatch,
        BREATHWISE_API_AUTH_ENABLED="false",
        BREATHWISE_RATE_LIMIT_ENABLED="false",
        BREATHWISE_API_CACHE_ENABLED="true",
        BREATHWISE_API_CACHE_TTL_SECONDS="300",
        BREATHWISE_API_CACHE_MAX_ENTRIES="128",
    )

    client = TestClient(main_module.app)

    first = client.get("/v1/cities/current", params={"limit": 2})
    second = client.get("/v1/cities/current", params={"limit": 2})

    assert first.status_code == 200
    assert second.status_code == 200
    assert first.headers.get("X-Breathwise-Cache") in {"miss", "disabled"}
    assert second.headers.get("X-Breathwise-Cache") in {"hit", "disabled"}


def test_access_log_enabled_writes_file(monkeypatch, tmp_path) -> None:
    log_path = tmp_path / "api_access.jsonl"
    main_module = load_app_with_env(
        monkeypatch,
        BREATHWISE_API_AUTH_ENABLED="false",
        BREATHWISE_RATE_LIMIT_ENABLED="false",
        BREATHWISE_API_CACHE_ENABLED="false",
        BREATHWISE_API_ACCESS_LOG_ENABLED="true",
        BREATHWISE_API_ACCESS_LOG_PATH=str(log_path),
    )

    client = TestClient(main_module.app)
    response = client.get("/v1/meta/datasets")
    assert response.status_code == 200
    assert log_path.exists()

    rows = [line for line in log_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert len(rows) >= 1
