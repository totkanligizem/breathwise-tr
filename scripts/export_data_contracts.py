from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from _shared import discover_project_root
from backend.schemas import (
    CityCurrentSnapshot,
    CityHourlyPoint,
    DatasetMeta,
    HealthResponse,
    LocalizationMeta,
    MobileCityTimelinePoint,
    MobileProvinceMapMetric,
    ProvinceMapMetric,
)


def parquet_columns(con: duckdb.DuckDBPyConnection, path: Path) -> list[dict[str, str]]:
    sql = f"describe select * from read_parquet('{path.as_posix()}')"
    rows = con.execute(sql).fetchall()
    return [
        {
            "column_name": row[0],
            "column_type": row[1],
            "null": row[2],
            "key": row[3],
            "default": row[4],
            "extra": row[5],
        }
        for row in rows
    ]


def write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def main() -> None:
    project_root = discover_project_root(Path(__file__))
    contracts_dir = project_root / "data" / "contracts"
    marts_dir = project_root / "data" / "processed" / "marts"
    views_dir = project_root / "data" / "processed" / "views"

    contracts_dir.mkdir(parents=True, exist_ok=True)

    model_map = {
        "health_response": HealthResponse,
        "dataset_meta": DatasetMeta,
        "localization_meta": LocalizationMeta,
        "city_current_snapshot": CityCurrentSnapshot,
        "city_hourly_point": CityHourlyPoint,
        "province_map_metric": ProvinceMapMetric,
        "mobile_city_timeline_point": MobileCityTimelinePoint,
        "mobile_province_map_metric": MobileProvinceMapMetric,
    }

    for name, model in model_map.items():
        write_json(
            contracts_dir / f"api_{name}.schema.json",
            {
                "generated_at_utc": datetime.now(timezone.utc).isoformat(),
                "model": model.__name__,
                "schema": model.model_json_schema(),
            },
        )

    i18n_contract = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "supported_locales": ["tr-TR", "en-US"],
        "default_locale": "tr-TR",
        "fallback_locale": "en-US",
        "translation_keys": {
            "aq.category.good": {"tr-TR": "İyi", "en-US": "Good"},
            "aq.category.fair": {"tr-TR": "Orta", "en-US": "Fair"},
            "aq.category.moderate": {"tr-TR": "Hassas Gruplar İçin Orta", "en-US": "Moderate"},
            "aq.category.poor": {"tr-TR": "Kötü", "en-US": "Poor"},
            "aq.category.very_poor": {"tr-TR": "Çok Kötü", "en-US": "Very Poor"},
            "aq.category.extremely_poor": {"tr-TR": "Aşırı Kötü", "en-US": "Extremely Poor"},
            "ui.city.current.title": {"tr-TR": "Güncel Durum", "en-US": "Current Conditions"},
            "ui.city.timeline.title": {"tr-TR": "Saatlik Zaman Çizelgesi", "en-US": "Hourly Timeline"},
            "ui.province.map.title": {"tr-TR": "İl Harita Metrikleri", "en-US": "Province Map Metrics"},
            "ui.forecast.reliability.title": {"tr-TR": "Tahmin Güvenilirliği", "en-US": "Forecast Reliability"},
            "ui.common.updated_at": {"tr-TR": "Güncellenme Zamanı", "en-US": "Updated At"},
            "alert.aq.clear": {"tr-TR": "AQ Uyarısı Yok", "en-US": "No AQ Alert"},
            "alert.aq.warning": {"tr-TR": "AQ Uyarısı", "en-US": "AQ Alert"},
            "alert.heat.clear": {"tr-TR": "Sıcaklık Uyarısı Yok", "en-US": "No Heat Alert"},
            "alert.heat.warning": {"tr-TR": "Sıcaklık Uyarısı", "en-US": "Heat Alert"},
            "ui.nav.city_overview": {"tr-TR": "Şehir Özeti", "en-US": "City Overview"},
            "ui.nav.city_timeline": {"tr-TR": "Saatlik Çizelge", "en-US": "Hourly Timeline"},
            "ui.nav.province_map": {"tr-TR": "İl Haritası", "en-US": "Province Map"},
            "ui.nav.settings": {"tr-TR": "Ayarlar", "en-US": "Settings"},
            "ui.settings.locale.title": {"tr-TR": "Dil Seçimi", "en-US": "Language Selection"},
            "ui.settings.locale.tr": {"tr-TR": "Türkçe", "en-US": "Turkish"},
            "ui.settings.locale.en": {"tr-TR": "İngilizce", "en-US": "English"},
            "ui.insight.forecast_vs_actual.title": {
                "tr-TR": "Tahmin ve Gerçek Karşılaştırması",
                "en-US": "Forecast vs Actual",
            },
        },
        "payload_key_guidance": {
            "pattern": "<domain>.<entity>.<label>",
            "example": "aq.category.good",
            "notes": [
                "User-facing labels should be delivered as stable translation keys where possible.",
                "Client applications resolve keys into TR/EN strings.",
                "API payloads should expose translation keys and optional localized labels for convenience.",
            ],
        },
        "app_payload_localization_fields": {
            "city_current_snapshot": {
                "key_field": "aq_category_key",
                "label_field": "aq_category_label",
                "locale_field": "label_locale",
            },
            "province_map_metrics": {
                "aq_key_field": "aq_alert_key",
                "aq_label_field": "aq_alert_label",
                "heat_key_field": "heat_alert_key",
                "heat_label_field": "heat_alert_label",
                "locale_field": "label_locale",
            },
        },
    }
    write_json(contracts_dir / "i18n_contract.json", i18n_contract)

    product_shell_view_models = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "version": "v1",
        "localization": {
            "translation_strategy": "translation-key-first",
            "supported_locales": i18n_contract["supported_locales"],
            "default_locale": i18n_contract["default_locale"],
            "fallback_locale": i18n_contract["fallback_locale"],
            "label_locale_field": "label_locale",
            "meta_endpoint": "/v1/meta/localization",
        },
        "screens": [
            {
                "screen_id": "city_current_overview",
                "title_key": "ui.city.current.title",
                "endpoint": "/v1/mobile/cities/current",
                "query_params": {"city_name": "optional", "limit": "optional", "locale": "optional"},
                "view_model_fields": [
                    "city_name",
                    "snapshot_time",
                    "forecast_temperature_2m",
                    "aq_european_aqi",
                    "aq_category",
                    "aq_category_key",
                    "aq_category_label",
                    "label_locale",
                ],
            },
            {
                "screen_id": "city_hourly_timeline",
                "title_key": "ui.city.timeline.title",
                "endpoint": "/v1/mobile/cities/{city_name}/timeline",
                "query_params": {"start": "optional", "end": "optional", "limit": "optional"},
                "view_model_fields": [
                    "city_name",
                    "time",
                    "forecast_temperature_2m",
                    "aq_european_aqi",
                    "aq_pm2_5",
                    "aq_pm10",
                ],
            },
            {
                "screen_id": "province_map_metrics",
                "title_key": "ui.province.map.title",
                "endpoint": "/v1/mobile/provinces/map-metrics",
                "query_params": {"limit": "optional", "locale": "optional"},
                "view_model_fields": [
                    "province_name",
                    "avg_aq_european_aqi",
                    "max_aq_european_aqi",
                    "avg_forecast_temperature_2m",
                    "map_priority_score",
                    "aq_alert_flag",
                    "aq_alert_key",
                    "aq_alert_label",
                    "heat_alert_flag",
                    "heat_alert_key",
                    "heat_alert_label",
                    "label_locale",
                ],
            },
            {
                "screen_id": "settings_locale",
                "title_key": "ui.settings.locale.title",
                "source": "localization_contract",
                "view_model_fields": ["supported_locales", "default_locale", "fallback_locale"],
            },
        ],
        "fallback_behavior": {
            "missing_label": "render_from_translation_key",
            "missing_translation_key": "render_raw_value_or_safe_placeholder",
            "unsupported_locale": "client_normalize_or_retry_with_default_locale",
        },
    }
    write_json(contracts_dir / "product_shell_view_models.json", product_shell_view_models)

    parquet_contract_targets = {
        "city_hourly_environment_tr": marts_dir / "city_hourly_environment_tr.parquet",
        "city_forecast_vs_actual_tr": marts_dir / "city_forecast_vs_actual_tr.parquet",
        "city_current_snapshot_tr": marts_dir / "city_current_snapshot_tr.parquet",
        "province_map_metrics_tr": marts_dir / "province_map_metrics_tr.parquet",
        "mobile_city_current_snapshot_tr_light": views_dir / "mobile_city_current_snapshot_tr_light.parquet",
        "mobile_city_hourly_timeline_tr_light": views_dir / "mobile_city_hourly_timeline_tr_light.parquet",
        "mobile_province_map_metrics_tr_light": views_dir / "mobile_province_map_metrics_tr_light.parquet",
    }

    con = duckdb.connect()
    try:
        def rel(path: Path) -> str:
            resolved = path.resolve()
            try:
                return resolved.relative_to(project_root).as_posix()
            except ValueError:
                return resolved.as_posix()

        parquet_manifest: dict[str, object] = {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "datasets": {},
        }

        for dataset_name, path in parquet_contract_targets.items():
            if not path.exists():
                parquet_manifest["datasets"][dataset_name] = {
                    "path": rel(path),
                    "status": "missing",
                }
                continue

            parquet_manifest["datasets"][dataset_name] = {
                "path": rel(path),
                "status": "present",
                "columns": parquet_columns(con, path),
            }

        write_json(contracts_dir / "parquet_contracts.json", parquet_manifest)

    finally:
        con.close()

    print(f"Contracts exported to: {contracts_dir}")


if __name__ == "__main__":
    main()
