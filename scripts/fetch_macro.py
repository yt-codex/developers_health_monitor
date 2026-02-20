#!/usr/bin/env python3
"""Fetch Singapore macro data for the dashboard via automatable public APIs.

Primary source: data.gov.sg CKAN API + CSV resources.
Secondary source: SingStat TableBuilder (optional; best effort).
"""

from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import requests

DATA_GOV_SG_API = "https://data.gov.sg/api/action"
SINGSTAT_BASE = "https://tablebuilder.singstat.gov.sg/api/table"


@dataclass
class SeriesIntent:
    id: str
    display_name: str
    frequency: str
    unit: str
    source: str
    candidate_queries: list[str]
    preferred_date_columns: list[str]
    preferred_value_columns: list[str]
    filters: dict[str, str] | None = None


SERIES_INTENTS: list[SeriesIntent] = [
    SeriesIntent(
        id="sora_level",
        display_name="SORA Level",
        frequency="daily",
        unit="%",
        source="data_gov_sg",
        candidate_queries=["SORA", "Singapore Overnight Rate Average", "interbank rate singapore"],
        preferred_date_columns=["date", "month", "period", "end_of_month"],
        preferred_value_columns=["sora", "rate", "interest rate", "value"],
    ),
    SeriesIntent(
        id="sgs_yield_2y",
        display_name="SGS Yield 2Y",
        frequency="daily",
        unit="%",
        source="data_gov_sg",
        candidate_queries=["SGS yield 2-year", "Singapore government securities yield", "MAS SGS yield"],
        preferred_date_columns=["date", "month", "period"],
        preferred_value_columns=["2-year", "2 year", "2y", "yield_2y", "value"],
    ),
    SeriesIntent(
        id="sgs_yield_10y",
        display_name="SGS Yield 10Y",
        frequency="daily",
        unit="%",
        source="data_gov_sg",
        candidate_queries=["SGS yield 10-year", "Singapore government bond yield 10y", "MAS SGS yield"],
        preferred_date_columns=["date", "month", "period"],
        preferred_value_columns=["10-year", "10 year", "10y", "yield_10y", "value"],
    ),
    SeriesIntent(
        id="sgs_yield_1y",
        display_name="T-bill / SGS Yield 1Y",
        frequency="daily",
        unit="%",
        source="data_gov_sg",
        candidate_queries=["SGS yield 1-year", "T-bill yield singapore", "MAS treasury bill"],
        preferred_date_columns=["date", "month", "period"],
        preferred_value_columns=["1-year", "1 year", "1y", "yield_1y", "value"],
    ),
    SeriesIntent(
        id="private_resi_price_index",
        display_name="Private Residential Property Price Index",
        frequency="quarterly",
        unit="index",
        source="data_gov_sg",
        candidate_queries=["private residential property price index singapore", "URA private residential price index"],
        preferred_date_columns=["quarter", "period", "date", "financial_quarter"],
        preferred_value_columns=["index", "price index", "residential property price", "value"],
    ),
    SeriesIntent(
        id="private_resi_rental_index",
        display_name="Private Residential Rental Index",
        frequency="quarterly",
        unit="index",
        source="data_gov_sg",
        candidate_queries=["private residential rental index singapore", "URA rental index private"],
        preferred_date_columns=["quarter", "period", "date"],
        preferred_value_columns=["index", "rental index", "value"],
    ),
    SeriesIntent(
        id="private_resi_transactions",
        display_name="Private Residential Transactions",
        frequency="quarterly",
        unit="transactions",
        source="data_gov_sg",
        candidate_queries=["private residential transactions singapore", "URA private residential sales transactions"],
        preferred_date_columns=["quarter", "period", "date"],
        preferred_value_columns=["transactions", "no. of transactions", "volume", "value"],
    ),
    SeriesIntent(
        id="dev_sales_uncompleted_sold",
        display_name="Uncompleted Private Residential Units Sold by Developers",
        frequency="quarterly",
        unit="units",
        source="data_gov_sg",
        candidate_queries=["uncompleted private residential units sold developers", "developers sales uncompleted units singapore"],
        preferred_date_columns=["quarter", "period", "date"],
        preferred_value_columns=["uncompleted", "units sold", "sold", "value"],
    ),
    SeriesIntent(
        id="dev_sales_completed_sold",
        display_name="Completed Private Residential Units Sold by Developers",
        frequency="quarterly",
        unit="units",
        source="data_gov_sg",
        candidate_queries=["completed private residential units sold developers", "developers sales completed units singapore"],
        preferred_date_columns=["quarter", "period", "date"],
        preferred_value_columns=["completed", "units sold", "sold", "value"],
    ),
    SeriesIntent(
        id="supply_starts",
        display_name="Private Residential Units Started",
        frequency="quarterly",
        unit="units",
        source="data_gov_sg",
        candidate_queries=["private residential units started singapore", "housing starts private residential singapore"],
        preferred_date_columns=["quarter", "period", "date", "year"],
        preferred_value_columns=["starts", "units started", "value"],
    ),
    SeriesIntent(
        id="supply_completions",
        display_name="Private Residential Units Completed",
        frequency="quarterly",
        unit="units",
        source="data_gov_sg",
        candidate_queries=["private residential units completed singapore", "housing completions private residential singapore"],
        preferred_date_columns=["quarter", "period", "date", "year"],
        preferred_value_columns=["completions", "units completed", "value"],
    ),
    SeriesIntent(
        id="supply_under_construction",
        display_name="Private Residential Units Under Construction",
        frequency="quarterly",
        unit="units",
        source="data_gov_sg",
        candidate_queries=["private residential units under construction singapore", "private housing under construction singapore"],
        preferred_date_columns=["quarter", "period", "date", "year"],
        preferred_value_columns=["under construction", "units", "value"],
    ),
    SeriesIntent(
        id="supply_available_vacant",
        display_name="Available-Vacant Private Residential Units",
        frequency="quarterly",
        unit="units",
        source="data_gov_sg",
        candidate_queries=["available vacant private residential units singapore", "vacant private housing stock singapore"],
        preferred_date_columns=["quarter", "period", "date", "year"],
        preferred_value_columns=["available-vacant", "vacant", "available", "value"],
    ),
    SeriesIntent(
        id="construction_material_prices",
        display_name="Construction Material Market Prices",
        frequency="monthly",
        unit="index_or_price",
        source="data_gov_sg",
        candidate_queries=["Construction Material Market Prices", "BCA construction material prices singapore", "construction materials market prices monthly"],
        preferred_date_columns=["month", "date", "period"],
        preferred_value_columns=["price", "index", "value", "average"],
    ),
]


class MacroFetcher:
    def __init__(self, timeout: int = 30):
        self.session = requests.Session()
        self.timeout = timeout
        self.source_status: dict[str, dict[str, Any]] = {
            "data_gov_sg": {"ok": True, "error": ""},
            "singstat": {"ok": True, "error": ""},
        }

    def package_search(self, query: str) -> list[dict[str, Any]]:
        url = f"{DATA_GOV_SG_API}/package_search"
        response = self.session.get(url, params={"q": query, "rows": 10}, timeout=self.timeout)
        response.raise_for_status()
        payload = response.json()
        if not payload.get("success"):
            raise RuntimeError(f"CKAN package_search unsuccessful for query: {query}")
        return payload.get("result", {}).get("results", [])

    def score_package(self, pkg: dict[str, Any], query: str) -> float:
        title = (pkg.get("title") or "").lower()
        org = ((pkg.get("organization") or {}).get("title") or "").lower()
        q = query.lower()
        score = 0.0
        if q == title:
            score += 30
        if q in title:
            score += 20
        tokens = [t for t in re.split(r"\W+", q) if len(t) > 2]
        score += sum(2 for t in tokens if t in title)

        if any(a in org for a in ("monetary authority", "mas", "urban redevelopment", "ura", "singstat", "bca")):
            score += 8

        if pkg.get("metadata_modified"):
            score += 5
        return score

    def resolve_dataset(self, intent: SeriesIntent) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
        best_pkg = None
        best_res = None
        best_score = -1.0

        for query in intent.candidate_queries:
            try:
                packages = self.package_search(query)
            except Exception as exc:  # noqa: BLE001
                self.source_status["data_gov_sg"] = {"ok": False, "error": truncate_error(str(exc))}
                continue

            for pkg in packages:
                resources = pkg.get("resources") or []
                csv_resources = [
                    r
                    for r in resources
                    if (r.get("format") or "").lower() == "csv" or (r.get("url") or "").lower().endswith(".csv")
                ]
                if not csv_resources:
                    continue
                score = self.score_package(pkg, query)
                if score > best_score:
                    best_pkg = pkg
                    best_res = csv_resources[0]
                    best_score = score

        return best_pkg, best_res

    def fetch_csv(self, resource_url: str) -> pd.DataFrame:
        return pd.read_csv(resource_url)



def normalize_period(value: Any, frequency: str) -> str | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None

    raw = str(value).strip()
    if not raw:
        return None

    quarter_match = re.search(r"(\d{4})\D*Q([1-4])", raw, flags=re.I)
    if quarter_match:
        return f"{quarter_match.group(1)}-Q{quarter_match.group(2)}"

    alt_quarter_match = re.search(r"(\d{4})\D*([1-4])Q", raw, flags=re.I)
    if alt_quarter_match:
        return f"{alt_quarter_match.group(1)}-Q{alt_quarter_match.group(2)}"

    if frequency == "quarterly":
        dt = pd.to_datetime(raw, errors="coerce")
        if pd.notna(dt):
            quarter = (dt.month - 1) // 3 + 1
            return f"{dt.year}-Q{quarter}"
        year_match = re.search(r"\b(\d{4})\b", raw)
        if year_match:
            return f"{year_match.group(1)}-Q1"
        return None

    dt = pd.to_datetime(raw, errors="coerce")
    if pd.isna(dt):
        return None

    if frequency == "monthly":
        return f"{dt.year:04d}-{dt.month:02d}-01"
    return dt.strftime("%Y-%m-%d")


def choose_column(columns: list[str], candidates: list[str]) -> str | None:
    normalized = {c: c.lower().strip() for c in columns}
    for candidate in candidates:
        cand = candidate.lower().strip()
        for original, low in normalized.items():
            if low == cand or cand in low:
                return original
    return None


def find_date_column(df: pd.DataFrame, intent: SeriesIntent) -> str | None:
    columns = list(df.columns)
    direct = choose_column(columns, intent.preferred_date_columns)
    if direct:
        return direct

    heuristics = ["date", "month", "quarter", "period", "year", "time"]
    return choose_column(columns, heuristics)


def find_value_column(df: pd.DataFrame, intent: SeriesIntent) -> str | None:
    columns = list(df.columns)
    preferred = choose_column(columns, intent.preferred_value_columns)
    if preferred:
        return preferred

    numeric_cols = []
    for col in columns:
        series = pd.to_numeric(df[col], errors="coerce")
        non_null = series.notna().sum()
        if non_null > 0:
            numeric_cols.append((col, non_null))

    if not numeric_cols:
        return None
    numeric_cols.sort(key=lambda x: x[1], reverse=True)
    return numeric_cols[0][0]


def apply_filters(df: pd.DataFrame, filters: dict[str, str] | None) -> pd.DataFrame:
    if not filters:
        return df

    out = df.copy()
    for col_hint, pattern in filters.items():
        candidate = choose_column(list(out.columns), [col_hint])
        if not candidate:
            continue
        out = out[out[candidate].astype(str).str.contains(pattern, case=False, na=False, regex=True)]
    return out


def parse_series(df: pd.DataFrame, intent: SeriesIntent) -> list[dict[str, Any]]:
    if df.empty:
        return []

    working = apply_filters(df, intent.filters)
    if working.empty:
        return []

    date_col = find_date_column(working, intent)
    value_col = find_value_column(working, intent)
    if not date_col or not value_col:
        return []

    values = pd.to_numeric(working[value_col], errors="coerce")
    periods = working[date_col].apply(lambda v: normalize_period(v, intent.frequency))

    result = pd.DataFrame({"period": periods, "value": values}).dropna(subset=["period", "value"])
    if result.empty:
        return []

    result = result.groupby("period", as_index=False).last().sort_values("period")
    return [
        {"period": row.period, "value": float(row.value)}
        for row in result.itertuples(index=False)
    ]


def build_empty_series(intent: SeriesIntent, note: str) -> dict[str, Any]:
    return {
        "display_name": intent.display_name,
        "frequency": intent.frequency,
        "unit": intent.unit,
        "source": {"name": "unavailable", "dataset_title": "", "resource_url": ""},
        "last_observation_period": None,
        "data": [],
        "note": note,
    }


def derive_curve_slope(series_map: dict[str, dict[str, Any]]) -> dict[str, Any] | None:
    ten = series_map.get("sgs_yield_10y", {}).get("data", [])
    two = series_map.get("sgs_yield_2y", {}).get("data", [])
    one = series_map.get("sgs_yield_1y", {}).get("data", [])

    base = two if two else one
    base_name = "2Y" if two else ("1Y" if one else None)
    if not ten or not base:
        return None

    ten_map = {row["period"]: row["value"] for row in ten}
    base_map = {row["period"]: row["value"] for row in base}
    shared = sorted(set(ten_map).intersection(base_map))
    data = [{"period": p, "value": float(ten_map[p] - base_map[p])} for p in shared]
    if not data:
        return None

    return {
        "display_name": f"Yield Curve Slope (10Y-{base_name})",
        "frequency": "daily",
        "unit": "percentage_points",
        "source": {"name": "derived", "dataset_title": "Derived from fetched yields", "resource_url": ""},
        "last_observation_period": data[-1]["period"],
        "data": data,
    }


def derive_moving_average(series_obj: dict[str, Any], window: int = 3) -> dict[str, Any] | None:
    src = series_obj.get("data") or []
    if len(src) < window:
        return None
    values = [row["value"] for row in src]
    periods = [row["period"] for row in src]

    ma_data = []
    for i in range(window - 1, len(values)):
        ma_data.append(
            {
                "period": periods[i],
                "value": float(sum(values[i - window + 1 : i + 1]) / window),
            }
        )

    return {
        "display_name": f"{series_obj['display_name']} ({window}-period MA)",
        "frequency": series_obj.get("frequency", "quarterly"),
        "unit": series_obj.get("unit", "value"),
        "source": {"name": "derived", "dataset_title": "Moving average", "resource_url": ""},
        "last_observation_period": ma_data[-1]["period"],
        "data": ma_data,
    }


def truncate_error(text: str, limit: int = 220) -> str:
    return text if len(text) <= limit else f"{text[:limit]}..."


def run_fetch() -> tuple[dict[str, Any], dict[str, Any]]:
    fetcher = MacroFetcher()
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    series_payload: dict[str, dict[str, Any]] = {}
    series_status: dict[str, dict[str, Any]] = {}
    notes: list[str] = [
        "Series are resolved dynamically from data.gov.sg using keyword queries.",
        "Missing datasets are tolerated; series remain present with empty data arrays.",
    ]

    for intent in SERIES_INTENTS:
        try:
            package, resource = fetcher.resolve_dataset(intent)
            if not package or not resource:
                series_payload[intent.id] = build_empty_series(intent, "No matching CSV dataset resource found")
                series_status[intent.id] = {"ok": False, "last_period": None, "error": "No dataset match"}
                continue

            resource_url = resource.get("url") or ""
            df = fetcher.fetch_csv(resource_url)
            points = parse_series(df, intent)
            if not points:
                series_payload[intent.id] = build_empty_series(intent, "Resolved dataset but no parseable data points")
                series_payload[intent.id]["source"] = {
                    "name": "data.gov.sg",
                    "dataset_title": package.get("title") or "",
                    "resource_url": resource_url,
                }
                series_status[intent.id] = {"ok": False, "last_period": None, "error": "No parseable data points"}
                continue

            series_payload[intent.id] = {
                "display_name": intent.display_name,
                "frequency": intent.frequency,
                "unit": intent.unit,
                "source": {
                    "name": "data.gov.sg",
                    "dataset_title": package.get("title") or "",
                    "resource_url": resource_url,
                },
                "last_observation_period": points[-1]["period"],
                "data": points,
            }
            series_status[intent.id] = {"ok": True, "last_period": points[-1]["period"], "error": ""}
        except Exception as exc:  # noqa: BLE001
            series_payload[intent.id] = build_empty_series(intent, "Fetch failed")
            series_status[intent.id] = {"ok": False, "last_period": None, "error": truncate_error(str(exc))}

    slope = derive_curve_slope(series_payload)
    if slope:
        series_payload["yield_curve_slope"] = slope
        series_status["yield_curve_slope"] = {
            "ok": True,
            "last_period": slope["last_observation_period"],
            "error": "",
        }

    for raw_id, derived_id in [
        ("private_resi_transactions", "private_resi_transactions_ma3"),
        ("dev_sales_uncompleted_sold", "dev_sales_uncompleted_sold_ma3"),
    ]:
        derived = derive_moving_average(series_payload.get(raw_id, {}), 3)
        if derived:
            series_payload[derived_id] = derived
            series_status[derived_id] = {
                "ok": True,
                "last_period": derived["last_observation_period"],
                "error": "",
            }

    macro_payload = {
        "meta": {
            "generated_utc": now,
            "sources": {
                "data_gov_sg": f"{DATA_GOV_SG_API}/package_search",
                "singstat": SINGSTAT_BASE,
            },
            "notes": notes,
        },
        "series": series_payload,
    }

    source_ok = all(v.get("ok") for v in fetcher.source_status.values())
    status_payload = {
        "last_run_utc": now,
        "ok": source_ok and all(v.get("ok") for v in series_status.values() if isinstance(v, dict)),
        "series_status": series_status,
        "source_status": fetcher.source_status,
        "errors": [v.get("error") for v in series_status.values() if v.get("error")][:20],
    }

    return macro_payload, status_payload


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch macro series for dashboard")
    parser.add_argument("--out", default="public/data/macro.json")
    parser.add_argument("--status", default="public/data/status.json")
    args = parser.parse_args()

    macro_payload, status_payload = run_fetch()
    write_json(Path(args.out), macro_payload)
    write_json(Path(args.status), status_payload)
    print(f"Wrote macro data to {args.out} and status to {args.status}")


if __name__ == "__main__":
    main()
