from __future__ import annotations

import importlib
from pathlib import Path

import pytest


def test_relative_paths_resolve_from_project_root(monkeypatch) -> None:
    import backend.config as config_module

    monkeypatch.setenv("BREATHWISE_MARTS_DIR", "data/custom/marts")
    monkeypatch.setenv("BREATHWISE_VIEWS_DIR", "data/custom/views")
    monkeypatch.setenv("BREATHWISE_DUCKDB_PATH", "data/custom/db.duckdb")
    monkeypatch.setenv("BREATHWISE_API_ACCESS_LOG_PATH", "data/custom/api.log")

    importlib.reload(config_module)
    config_module.get_settings.cache_clear()

    settings = config_module.get_settings()

    assert settings.marts_dir == (settings.project_root / "data/custom/marts").resolve()
    assert settings.views_dir == (settings.project_root / "data/custom/views").resolve()
    assert settings.duckdb_path == (settings.project_root / "data/custom/db.duckdb").resolve()
    assert settings.api_access_log_path == (settings.project_root / "data/custom/api.log").resolve()


def test_invalid_int_env_raises_runtime_error(monkeypatch) -> None:
    import backend.config as config_module

    monkeypatch.setenv("BREATHWISE_RATE_LIMIT_REQUESTS", "not-an-int")

    importlib.reload(config_module)
    config_module.get_settings.cache_clear()

    with pytest.raises(RuntimeError):
        _ = config_module.get_settings()
