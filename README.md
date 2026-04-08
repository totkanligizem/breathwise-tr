# Breathwise TR

Türkiye geneli çevresel zeka platformu (local-first): analitik veri katmanı, FastAPI servisleri ve TR/EN mobil-web ürün kabuğu.

English README: [README.eng.md](README.eng.md)

## Yönetici Özeti
- **Ne:** Hava durumu, hava kalitesi, tarihsel gerçekleşenler, tarihsel forecast arşivi, CAMS reanalysis ve il geometrisini birleştiren veri+API sistemi.
- **Ne üretir:** Şehir/il martları, mobil hafif görünümler, auth korumalı API, zamanlanabilir pipeline ve operasyon görünürlüğü.
- **Durum:** Validasyon/test tabanı yeşil, üretim disiplini güçlü, local-first mimari aktif.
- **İlke:** Forecast ve actual semantiği ayrıdır; joinler deterministik ve tekrarlanabilir.
- **Güvenlik:** Sadece environment tabanlı secret, publish-safe yüzeyler, sanitize export zorunluluğu.

## Kapsam, Amaç, Hedef
- **Kapsam:** 81 il, şehir + il seviyesinde çevresel analiz.
- **Amaç:** Dashboard, mobil/web ürün yüzeyi ve model/agent akışları için güvenilir veri kontratları.
- **Hedefler:**
  - Şehir saatlik birleşik çevresel zeka
  - Forecast-vs-actual güvenilirlik analizi
  - İl bazlı harita metrikleri
  - Tekrarlanabilir operasyonel refresh

## Entegre Veri Kaynakları
- Open-Meteo Geocoding (81 il)
- Open-Meteo Forecast
- Open-Meteo Air Quality
- Open-Meteo Historical Weather
- Open-Meteo Historical Forecast (2024 extended canonical)
- geoBoundaries Turkey ADM0/ADM1/ADM2
- CAMS reanalysis (2024 aylık kapsam)

## Teknoloji Yığını
- **Diller:** Python, TypeScript
- **Analitik:** DuckDB, Parquet, Pandas, PyArrow
- **Coğrafi:** GeoPandas, Shapely, PyProj, Fiona
- **Atmosferik dosya işleme:** NetCDF4, Xarray
- **Backend:** FastAPI, Uvicorn
- **Frontend/Mobil:** Expo, React Native, React Native Web, React Native SVG
- **Kalite:** Pytest, TypeScript typecheck
- **Operasyon:** cron/launchd/systemd şablonları, logrotate, run manifestleri

## Repository Yapısı
```text
breathwise-tr/
  backend/                        # FastAPI servis, config, schema
  frontend/mobile_shell_starter/  # Çalışır Expo uygulama kabuğu (TR/EN)
  scripts/                        # ETL, mart build, orchestrator, doğrulama
  tests/                          # Backend/pipeline/ops regresyon testleri
  data/
    raw/                          # Immutable kaynak artifaktları
    processed/                    # Martlar, viewler, run/ops çıktıları
    contracts/                    # API/parquet/i18n/product kontratları
  ops/                            # Scheduler ve logrotate şablonları
  sql/                            # SQL yardımcıları
  *.md                            # Proje dokümantasyonu
```

## Ana Çıktılar
- `data/processed/marts/`
  - `city_hourly_environment_tr.parquet`
  - `city_forecast_vs_actual_tr.parquet`
  - `city_current_snapshot_tr.parquet`
  - `province_map_metrics_tr.parquet`
- `data/processed/views/`
  - `mobile_city_current_snapshot_tr_light.parquet`
  - `mobile_city_hourly_timeline_tr_light.parquet`
  - `mobile_province_map_metrics_tr_light.parquet`

## Hızlı Başlangıç
1. Python ortamı:
   - `python3 -m venv .venv && source .venv/bin/activate`
   - `pip install -r requirements.txt`
2. Veri doğrulama:
   - `python3 scripts/validate_analytics_outputs.py`
3. API çalıştırma:
   - `uvicorn backend.main:app --reload --host 127.0.0.1 --port 8000`
4. Mobil/web kabuk:
   - `cd frontend/mobile_shell_starter`
   - `npm install && npm run start`

## Güvenlik ve Paylaşım Disiplini
- Secret hardcode edilmez.
- Sadece environment variable kullanılır (`OPENAI_API_KEY`, `BREATHWISE_API_KEY` vb.).
- `.env` commit edilmez; sadece `.env.example` tutulur.
- Dış paylaşımda metadata için `publish_safe=true` kullanılır.
- Raw kaynaklar immutable’dır; sanitize işlemi sunum/export katmanında yapılır.

Standart sanitize export:
```bash
python3 scripts/export_publish_bundle.py --tag release_candidate --overwrite
```
Çıktı: `data/processed/publish_exports/<tag>/`

**Kanonik kural:** Public/portfolio paylaşım sadece sanitize bundle’dan yapılır.

## Doküman Haritası
- [DATA_PIPELINE_OVERVIEW.md](DATA_PIPELINE_OVERVIEW.md)
- [DATASET_CATALOG.md](DATASET_CATALOG.md)
- [DATA_DICTIONARY.md](DATA_DICTIONARY.md)
- [JOIN_STRATEGY.md](JOIN_STRATEGY.md)
- [ANALYTICS_LAYER.md](ANALYTICS_LAYER.md)
- [OPERATIONS_RUNBOOK.md](OPERATIONS_RUNBOOK.md)
- [LOCALIZATION_STRATEGY.md](LOCALIZATION_STRATEGY.md)
- [PRODUCT_SHELL_INTEGRATION.md](PRODUCT_SHELL_INTEGRATION.md)
- [SECURITY.md](SECURITY.md)
- [THIRD_PARTY_SOURCES.md](THIRD_PARTY_SOURCES.md)
- [RELEASE_READINESS_CHECKLIST.md](RELEASE_READINESS_CHECKLIST.md)
- [RELEASE_WORKFLOW.md](RELEASE_WORKFLOW.md)
- [ROADMAP_2025_PLUS.md](ROADMAP_2025_PLUS.md)

## Kısa Sonraki Plan
1. Sanitized export adımını release sürecinde standartlaştırmaya devam.
2. 2025+ veri genişlemesi (aynı validasyon sertliğiyle).
3. UI polish + ürün release checklist (store/share readiness).
