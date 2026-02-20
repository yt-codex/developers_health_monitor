# Singapore Property Developer Vulnerability Monitor (POC)

GitHub Pages-hosted static dashboard for monitoring vulnerability signals across Singapore property developers.

## What this includes

- **Static frontend** (`/public`) with 3 dashboard pages.
- **Macro data pipeline** (`scripts/fetch_macro.py`) that resolves and fetches macro datasets from **data.gov.sg CKAN API** (plus optional SingStat endpoint reference for future extension).
- **Mock listed/news pipeline** (`scripts/build_data.py`) for deterministic POC content.
- **GitHub Actions job** (`.github/workflows/build-data.yml`) to regenerate and commit JSON assets.

## Macro pipeline design

### 1) Series resolution

`fetch_macro.py` hardcodes only **series intents**, not brittle resource IDs. Each intent defines:

- `id`
- `display_name`
- `frequency`
- `source`
- `candidate_queries`
- `preferred_date_columns`
- `preferred_value_columns`

Resolver flow:

1. Call `package_search` for each candidate query.
2. Score package matches using title token overlap, agency hints (MAS/URA/SingStat/BCA), and metadata freshness.
3. Pick the best package and first CSV resource.
4. Download CSV and parse with pandas.

### 2) Parsing + normalization

- Date/period fields are detected via preferred column names + heuristics.
- Values are parsed numerically with NA handling.
- Period format normalization:
  - daily/monthly => ISO date (`YYYY-MM-DD`)
  - quarterly => `YYYY-Q#`
- Series are sorted ascending and deduplicated by period.

### 3) Derived metrics

- `yield_curve_slope = yield_10y - yield_2y` (or `yield_10y - yield_1y` fallback).
- Optional 3-period MAs for transactions and developers' uncompleted sales when enough history exists.

### 4) Status and resilience

Pipeline writes:

- `public/data/macro.json`
- `public/data/status.json`

The pipeline is best-effort: if a dataset is missing/unparseable, series are emitted with empty `data` and the failure is recorded in `status.json`.

## Macro JSON schema

`macro.json`:

```json
{
  "meta": {"generated_utc": "...", "sources": {}, "notes": []},
  "series": {
    "series_id": {
      "display_name": "...",
      "frequency": "daily|monthly|quarterly",
      "unit": "...",
      "source": {"name": "data.gov.sg", "dataset_title": "...", "resource_url": "..."},
      "last_observation_period": "...",
      "data": [{"period": "...", "value": 123.4}]
    }
  }
}
```

`status.json`:

```json
{
  "last_run_utc": "...",
  "ok": false,
  "series_status": {"series_id": {"ok": false, "last_period": null, "error": "..."}},
  "source_status": {"data_gov_sg": {"ok": true, "error": ""}, "singstat": {"ok": true, "error": ""}},
  "errors": []
}
```

## How to add a new macro series

1. Open `scripts/fetch_macro.py`.
2. Add a new `SeriesIntent` in `SERIES_INTENTS`.
3. Fill in candidate queries and preferred columns.
4. (Optional) add filters/derived logic.
5. Run:

```bash
python scripts/fetch_macro.py --out public/data/macro.json --status public/data/status.json
```

## Troubleshooting

- **Series missing**: broaden `candidate_queries`, adjust `preferred_*_columns`.
- **Wrong values**: check value column inference and add stronger preferred names.
- **Quarter parsing issues**: update `normalize_period` regex for new source formats.
- **Workflow no commit**: verify `public/` or `docs/` actually changed.

## Local run

```bash
python scripts/build_data.py
python scripts/fetch_macro.py --out public/data/macro.json --status public/data/status.json
cd public && python -m http.server 8000
```

Open `http://localhost:8000/macro.html`.
