"""Pipeline steps exposed to the ADK agents as tools.

Each function is one stage of the pipeline:

    1. fetch_weather_data      -> ingestion (public API)
    2. transform_weather_data  -> processing (filter, group, aggregate)
    3. upload_to_gcs           -> storage (Google Cloud Storage)
    4. build_html_report       -> visualization (HTML page)

The agents call these tools. Data moves between stages through the ADK
session state (``tool_context.state``), so large data never has to pass
through the language model itself. Each stage also writes its output to
the local ``output/`` folder so the results can be inspected.
"""

from __future__ import annotations

import csv
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import date
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import Any

from google.adk.tools import ToolContext

# --------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------

# Cities to compare. Edit this dictionary to analyze different places.
CITIES: dict[str, tuple[float, float]] = {
    "Bloomington, IN": (39.1653, -86.5264),
    "Indianapolis, IN": (39.7684, -86.1581),
    "Chicago, IL": (41.8781, -87.6298),
    "Denver, CO": (39.7392, -104.9903),
}

# Open-Meteo is free and needs no API key.
API_URL = "https://api.open-meteo.com/v1/forecast"
DAILY_FIELDS = [
    "temperature_2m_max",
    "temperature_2m_min",
    "precipitation_sum",
    "wind_speed_10m_max",
]
LOOKBACK_DAYS = 30
WET_DAY_THRESHOLD_IN = 0.01

PROJECT_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_DIR = PROJECT_ROOT / "output"
RAW_FILE = OUTPUT_DIR / "raw_weather.json"
CLEAN_FILE = OUTPUT_DIR / "daily_clean.csv"
SUMMARY_FILE = OUTPUT_DIR / "summary.json"
REPORT_FILE = OUTPUT_DIR / "report.html"


def _error(message: str) -> dict[str, Any]:
  return {"status": "error", "message": message}


def _get_json(url: str, retries: int = 3) -> dict[str, Any]:
  """Downloads JSON from a URL, retrying briefly on network errors."""
  request = urllib.request.Request(
      url, headers={"User-Agent": "da-agentic-pipeline/1.0"}
  )
  for attempt in range(1, retries + 1):
    try:
      with urllib.request.urlopen(request, timeout=30) as response:
        return json.loads(response.read().decode("utf-8"))
    except (urllib.error.URLError, TimeoutError) as exc:
      if attempt == retries:
        raise
      wait = 2**attempt
      print(f"  API request failed ({exc}), retrying in {wait}s...")
      time.sleep(wait)
  raise RuntimeError("unreachable")


# --------------------------------------------------------------------------
# Step 1: Ingestion
# --------------------------------------------------------------------------


def fetch_weather_data(tool_context: ToolContext) -> dict[str, Any]:
  """Fetches the last 30 days of daily weather for each configured city.

  Pulls daily high and low temperature (F), precipitation (inches), and
  maximum wind speed (mph) from the free Open-Meteo API. Saves the raw API
  responses to output/raw_weather.json and to the session state.

  Returns:
      A short status summary of what was fetched.
  """
  OUTPUT_DIR.mkdir(exist_ok=True)
  run_date = date.today().isoformat()
  raw: dict[str, Any] = {}

  for city, (lat, lon) in CITIES.items():
    params = {
        "latitude": lat,
        "longitude": lon,
        "daily": ",".join(DAILY_FIELDS),
        "past_days": LOOKBACK_DAYS,
        "forecast_days": 1,
        "temperature_unit": "fahrenheit",
        "precipitation_unit": "inch",
        "wind_speed_unit": "mph",
        "timezone": "auto",
    }
    url = f"{API_URL}?{urllib.parse.urlencode(params)}"
    try:
      raw[city] = _get_json(url)
    except Exception as exc:  # noqa: BLE001 - report any failure to the agent
      return _error(f"Could not fetch data for {city}: {exc}")

  RAW_FILE.write_text(json.dumps(raw, indent=2), encoding="utf-8")
  tool_context.state["raw_weather"] = raw
  tool_context.state["run_date"] = run_date

  days = {city: len(data.get("daily", {}).get("time", [])) for city, data in raw.items()}
  return {
      "status": "success",
      "run_date": run_date,
      "cities_fetched": list(raw.keys()),
      "days_per_city": days,
      "saved_to": str(RAW_FILE.relative_to(PROJECT_ROOT)),
  }


# --------------------------------------------------------------------------
# Step 2: Processing
# --------------------------------------------------------------------------


def _week_start(day: date) -> str:
  return (day - timedelta(days=day.weekday())).isoformat()


def transform_weather_data(tool_context: ToolContext) -> dict[str, Any]:
  """Cleans, filters, groups, and aggregates the raw weather data.

  Filtering: keeps only completed days (drops today's partial forecast) and
  drops rows with missing temperatures.
  Grouping and aggregation: computes per-city statistics (average high and
  low, temperature swing, record high and low, total precipitation, number
  of wet days, and maximum wind) plus weekly average highs per city.

  Returns:
      Per-city summary statistics and data quality counts.
  """
  raw = tool_context.state.get("raw_weather")
  run_date_str = tool_context.state.get("run_date")
  if not raw or not run_date_str:
    return _error("No raw data found. The ingestion step must run first.")
  run_date = date.fromisoformat(run_date_str)

  rows: list[dict[str, Any]] = []
  dropped_incomplete = 0
  dropped_missing = 0

  for city, payload in raw.items():
    daily = payload.get("daily", {})
    for i, day_str in enumerate(daily.get("time", [])):
      day = date.fromisoformat(day_str)
      if day >= run_date:
        dropped_incomplete += 1
        continue
      high = daily["temperature_2m_max"][i]
      low = daily["temperature_2m_min"][i]
      if high is None or low is None:
        dropped_missing += 1
        continue
      rows.append({
          "city": city,
          "date": day_str,
          "week_start": _week_start(day),
          "high_f": round(high, 1),
          "low_f": round(low, 1),
          "precip_in": round(daily["precipitation_sum"][i] or 0.0, 2),
          "wind_max_mph": round(daily["wind_speed_10m_max"][i] or 0.0, 1),
      })

  if not rows:
    return _error("No usable rows remained after cleaning.")

  # Per-city aggregation.
  cities: dict[str, dict[str, Any]] = {}
  for city in raw:
    city_rows = [r for r in rows if r["city"] == city]
    if not city_rows:
      continue
    highs = [r["high_f"] for r in city_rows]
    lows = [r["low_f"] for r in city_rows]
    hottest = max(city_rows, key=lambda r: r["high_f"])
    coldest = min(city_rows, key=lambda r: r["low_f"])
    cities[city] = {
        "days": len(city_rows),
        "avg_high_f": round(sum(highs) / len(highs), 1),
        "avg_low_f": round(sum(lows) / len(lows), 1),
        "avg_daily_swing_f": round(
            sum(h - l for h, l in zip(highs, lows)) / len(highs), 1
        ),
        "record_high_f": hottest["high_f"],
        "record_high_date": hottest["date"],
        "record_low_f": coldest["low_f"],
        "record_low_date": coldest["date"],
        "total_precip_in": round(sum(r["precip_in"] for r in city_rows), 2),
        "wet_days": sum(1 for r in city_rows if r["precip_in"] >= WET_DAY_THRESHOLD_IN),
        "max_wind_mph": max(r["wind_max_mph"] for r in city_rows),
    }

  # Weekly grouping: average high per city per week.
  weekly: dict[str, dict[str, float]] = {}
  for week in sorted({r["week_start"] for r in rows}):
    weekly[week] = {}
    for city in cities:
      week_highs = [r["high_f"] for r in rows if r["city"] == city and r["week_start"] == week]
      if week_highs:
        weekly[week][city] = round(sum(week_highs) / len(week_highs), 1)

  summary = {
      "run_date": run_date_str,
      "period_start": min(r["date"] for r in rows),
      "period_end": max(r["date"] for r in rows),
      "cities": cities,
      "weekly_avg_high_f": weekly,
      "warmest_city": max(cities, key=lambda c: cities[c]["avg_high_f"]),
      "wettest_city": max(cities, key=lambda c: cities[c]["total_precip_in"]),
      "data_quality": {
          "rows_kept": len(rows),
          "rows_dropped_incomplete_day": dropped_incomplete,
          "rows_dropped_missing_values": dropped_missing,
      },
  }

  with CLEAN_FILE.open("w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
    writer.writeheader()
    writer.writerows(rows)
  SUMMARY_FILE.write_text(json.dumps(summary, indent=2), encoding="utf-8")

  tool_context.state["daily_rows"] = rows
  tool_context.state["summary"] = summary

  return {
      "status": "success",
      "period": f"{summary['period_start']} to {summary['period_end']}",
      "warmest_city": summary["warmest_city"],
      "wettest_city": summary["wettest_city"],
      "city_stats": cities,
      "data_quality": summary["data_quality"],
  }


# --------------------------------------------------------------------------
# Step 3: Storage
# --------------------------------------------------------------------------


def _run_prefix(run_date: str) -> str:
  # One folder per run date. Re-running on the same day overwrites the same
  # objects instead of creating duplicates, so the step is idempotent.
  return f"weather_pipeline/run_date={run_date}"


def _upload(bucket, local_path: Path, blob_name: str, content_type: str) -> str:
  blob = bucket.blob(blob_name)
  blob.upload_from_filename(str(local_path), content_type=content_type)
  return f"gs://{bucket.name}/{blob_name}"


def upload_to_gcs(tool_context: ToolContext) -> dict[str, Any]:
  """Uploads the raw and processed data files to Google Cloud Storage.

  Uses the bucket named in the GCS_BUCKET environment variable. Files are
  written under weather_pipeline/run_date=YYYY-MM-DD/ in a raw/ folder and a
  processed/ folder.

  Returns:
      The gs:// locations of the uploaded files.
  """
  bucket_name = os.getenv("GCS_BUCKET")
  run_date = tool_context.state.get("run_date")
  if not bucket_name:
    return _error("GCS_BUCKET is not set. Add it to your .env file.")
  if not run_date or not SUMMARY_FILE.exists():
    return _error("No processed data found. The processing step must run first.")

  try:
    from google.cloud import storage

    client = storage.Client(project=os.getenv("GOOGLE_CLOUD_PROJECT") or None)
    bucket = client.bucket(bucket_name)
    prefix = _run_prefix(run_date)
    uris = [
        _upload(bucket, RAW_FILE, f"{prefix}/raw/raw_weather.json", "application/json"),
        _upload(bucket, CLEAN_FILE, f"{prefix}/processed/daily_clean.csv", "text/csv"),
        _upload(bucket, SUMMARY_FILE, f"{prefix}/processed/summary.json", "application/json"),
    ]
  except Exception as exc:  # noqa: BLE001 - report any failure to the agent
    return _error(f"Upload to bucket '{bucket_name}' failed: {exc}")

  tool_context.state["gcs_uris"] = uris
  return {"status": "success", "uploaded": uris}


# --------------------------------------------------------------------------
# Step 4: Visualization
# --------------------------------------------------------------------------

_HTML_TEMPLATE = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Weather Pipeline Report</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
<style>
  body {{ font-family: -apple-system, Segoe UI, Roboto, sans-serif; margin: 0; background: #f6f8fa; color: #1f2328; }}
  main {{ max-width: 1000px; margin: 0 auto; padding: 32px 16px; }}
  h1 {{ margin: 0 0 4px; }}
  .sub {{ color: #59636e; margin: 0 0 24px; }}
  .cards {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 12px; margin-bottom: 24px; }}
  .card, .panel {{ background: #fff; border: 1px solid #d0d7de; border-radius: 10px; padding: 16px; }}
  .card .label {{ color: #59636e; font-size: 14px; }}
  .card .value {{ font-size: 22px; font-weight: 600; margin-top: 4px; }}
  .panel {{ margin-bottom: 24px; }}
  h2 {{ font-size: 18px; margin: 0 0 12px; }}
  table {{ width: 100%; border-collapse: collapse; font-size: 14px; }}
  th, td {{ text-align: left; padding: 8px; border-bottom: 1px solid #eaeef2; }}
  th {{ background: #f6f8fa; }}
  .foot {{ color: #59636e; font-size: 13px; }}
</style>
</head>
<body>
<main>
  <h1>City Weather Comparison</h1>
  <p class="sub">Daily weather from {period_start} to {period_end} &middot; Source: Open-Meteo &middot; Generated {generated}</p>

  <div class="cards">
    <div class="card"><div class="label">Warmest city (avg high)</div><div class="value">{warmest_city}</div></div>
    <div class="card"><div class="label">Wettest city (total precip)</div><div class="value">{wettest_city}</div></div>
    <div class="card"><div class="label">Days analyzed per city</div><div class="value">{days}</div></div>
    <div class="card"><div class="label">Rows kept / dropped</div><div class="value">{rows_kept} / {rows_dropped}</div></div>
  </div>

  <div class="panel"><h2>Average daily high and low (&deg;F)</h2><canvas id="avgChart" height="110"></canvas></div>
  <div class="panel"><h2>Daily high temperature (&deg;F)</h2><canvas id="dailyChart" height="110"></canvas></div>
  <div class="panel"><h2>Total precipitation (inches)</h2><canvas id="precipChart" height="90"></canvas></div>

  <div class="panel">
    <h2>Summary by city</h2>
    <table>
      <thead><tr><th>City</th><th>Avg high</th><th>Avg low</th><th>Avg swing</th><th>Record high</th><th>Record low</th><th>Total precip</th><th>Wet days</th><th>Max wind</th></tr></thead>
      <tbody>{table_rows}</tbody>
    </table>
  </div>

  <p class="foot">Built by an ADK agent pipeline: ingestion &rarr; processing &rarr; Cloud Storage &rarr; this report.</p>
</main>
<script>
const summary = {summary_json};
const daily = {daily_json};
const cities = Object.keys(summary.cities);
const palette = ["#2563eb", "#dc2626", "#16a34a", "#9333ea", "#ea580c", "#0891b2"];

new Chart(document.getElementById("avgChart"), {{
  type: "bar",
  data: {{
    labels: cities,
    datasets: [
      {{ label: "Avg high", data: cities.map(c => summary.cities[c].avg_high_f), backgroundColor: "#ef4444" }},
      {{ label: "Avg low", data: cities.map(c => summary.cities[c].avg_low_f), backgroundColor: "#3b82f6" }}
    ]
  }}
}});

const dates = [...new Set(daily.map(r => r.date))].sort();
new Chart(document.getElementById("dailyChart"), {{
  type: "line",
  data: {{
    labels: dates,
    datasets: cities.map((c, i) => ({{
      label: c,
      data: dates.map(d => {{ const r = daily.find(x => x.city === c && x.date === d); return r ? r.high_f : null; }}),
      borderColor: palette[i % palette.length],
      backgroundColor: palette[i % palette.length],
      tension: 0.25,
      pointRadius: 2
    }}))
  }}
}});

new Chart(document.getElementById("precipChart"), {{
  type: "bar",
  data: {{
    labels: cities,
    datasets: [{{ label: "Total precipitation (in)", data: cities.map(c => summary.cities[c].total_precip_in), backgroundColor: "#0891b2" }}]
  }},
  options: {{ indexAxis: "y" }}
}});
</script>
</body>
</html>
"""


def build_html_report(tool_context: ToolContext) -> dict[str, Any]:
  """Builds an HTML report with charts and a summary table.

  Writes output/report.html. If GCS_BUCKET is set, also uploads the report
  to the bucket next to the data.

  Returns:
      The local path of the report and, if uploaded, its Cloud Storage link.
  """
  summary = tool_context.state.get("summary")
  rows = tool_context.state.get("daily_rows")
  if not summary or not rows:
    return _error("No processed data found. The processing step must run first.")

  table_rows = "".join(
      "<tr>"
      f"<td>{city}</td>"
      f"<td>{s['avg_high_f']}&deg;</td>"
      f"<td>{s['avg_low_f']}&deg;</td>"
      f"<td>{s['avg_daily_swing_f']}&deg;</td>"
      f"<td>{s['record_high_f']}&deg; ({s['record_high_date']})</td>"
      f"<td>{s['record_low_f']}&deg; ({s['record_low_date']})</td>"
      f"<td>{s['total_precip_in']} in</td>"
      f"<td>{s['wet_days']}</td>"
      f"<td>{s['max_wind_mph']} mph</td>"
      "</tr>"
      for city, s in summary["cities"].items()
  )
  quality = summary["data_quality"]
  html = _HTML_TEMPLATE.format(
      period_start=summary["period_start"],
      period_end=summary["period_end"],
      generated=datetime.now().strftime("%Y-%m-%d %H:%M"),
      warmest_city=summary["warmest_city"],
      wettest_city=summary["wettest_city"],
      days=max(s["days"] for s in summary["cities"].values()),
      rows_kept=quality["rows_kept"],
      rows_dropped=quality["rows_dropped_incomplete_day"] + quality["rows_dropped_missing_values"],
      table_rows=table_rows,
      summary_json=json.dumps(summary),
      daily_json=json.dumps(rows),
  )
  OUTPUT_DIR.mkdir(exist_ok=True)
  REPORT_FILE.write_text(html, encoding="utf-8")
  result: dict[str, Any] = {
      "status": "success",
      "report_path": str(REPORT_FILE.relative_to(PROJECT_ROOT)),
  }

  bucket_name = os.getenv("GCS_BUCKET")
  if bucket_name:
    try:
      from google.cloud import storage

      client = storage.Client(project=os.getenv("GOOGLE_CLOUD_PROJECT") or None)
      blob_name = f"{_run_prefix(summary['run_date'])}/report/report.html"
      uri = _upload(client.bucket(bucket_name), REPORT_FILE, blob_name, "text/html")
      result["gcs_uri"] = uri
      result["view_in_browser"] = f"https://storage.cloud.google.com/{bucket_name}/{blob_name}"
    except Exception as exc:  # noqa: BLE001 - local report still succeeded
      result["upload_warning"] = f"Report saved locally but upload failed: {exc}"

  return result
