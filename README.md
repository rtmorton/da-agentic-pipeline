# Agentic Weather Data Pipeline (Google ADK)

An end-to-end data engineering pipeline orchestrated by AI agents built with
Google's Agent Development Kit (ADK). The pipeline pulls 30 days of daily
weather for four cities from a public API, cleans and aggregates it, stores the
raw and processed data in Google Cloud Storage, and produces an HTML report
with charts and a summary table.

## How it works

```
weather_pipeline (ADK SequentialAgent)
 ├─ 1. ingestion_agent      → fetch_weather_data      Open-Meteo API → output/raw_weather.json
 ├─ 2. processing_agent     → transform_weather_data  filter, group, aggregate → daily_clean.csv, summary.json
 ├─ 3. storage_agent        → upload_to_gcs           raw/ and processed/ files → Google Cloud Storage
 └─ 4. visualization_agent  → build_html_report       output/report.html (also uploaded to the bucket)
```

- **Agentic design:** a `SequentialAgent` coordinates four specialist
  `LlmAgent`s. Each agent owns one stage, calls its tool, checks the result,
  and reports back. Data passes between stages through the ADK session state,
  so the large datasets never go through the language model.
- **Ingestion:** [Open-Meteo](https://open-meteo.com/) (free, no API key)
  provides daily high and low temperature, precipitation, and maximum wind for
  Bloomington IN, Indianapolis IN, Chicago IL, and Denver CO.
- **Processing:**
  - *Filtering:* drops today's incomplete forecast day and any rows with
    missing temperatures, and reports how many rows were dropped.
  - *Grouping and aggregation:* per-city average high and low, average daily
    temperature swing, record high and low (with dates), total precipitation,
    number of wet days, and maximum wind, plus weekly average highs per city.
- **Storage:** files are written to
  `gs://<bucket>/weather_pipeline/run_date=YYYY-MM-DD/{raw,processed,report}/`.
  Re-running on the same day overwrites the same files instead of creating
  duplicates, so the storage step is idempotent.
- **Visualization:** `output/report.html` shows summary cards, a bar chart of
  average highs and lows, a line chart of daily highs, a precipitation chart,
  and a summary table.

## Project structure

```
.
├── main.py                  # Runs the pipeline from the command line
├── pipeline_agent/
│   ├── __init__.py
│   ├── agent.py             # ADK agent definitions (root_agent)
│   └── tools.py             # The four pipeline steps used as agent tools
├── env.example              # Template for your .env settings file
├── requirements.txt
└── README.md
```

## Prerequisites

1. **Python 3.10 or newer.**
2. **A Gemini API key** (free) from <https://aistudio.google.com/apikey>.
3. **A Google Cloud project** with a Cloud Storage bucket you can write to.
4. **Google Cloud credentials** for that project:
   - In **Google Cloud Shell**, this is already done for you.
   - On your own computer, install the
     [gcloud CLI](https://cloud.google.com/sdk/docs/install) and run
     `gcloud auth application-default login`.

## Setup and run

The quickest option is Google Cloud Shell (the terminal icon at the top right
of the Google Cloud console), which already has Python, git, and credentials.

```bash
# 1. Get the code
git clone <this-repository-url>
cd da-agentic-pipeline

# 2. Install dependencies
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 3. Create your settings file and fill in your own values
cp env.example .env
nano .env        # set GOOGLE_API_KEY, GOOGLE_CLOUD_PROJECT, and GCS_BUCKET

# 4. Run the pipeline
python main.py
```

The terminal shows each agent calling its tool and reporting its result.
When it finishes:

- Local copies of every output are in the `output/` folder.
- The data and the report are in your bucket under
  `weather_pipeline/run_date=<today>/`.
- The final agent message includes a browser link to the report in Cloud
  Storage. You can also download `output/report.html` and open it in any
  browser.

You can also explore the agents interactively with ADK's web UI by running
`adk web` from the project folder, choosing `pipeline_agent`, and typing
"Run the weather data pipeline."

## Configuration

| Setting | Required | Description |
|---|---|---|
| `GOOGLE_API_KEY` | Yes | Gemini API key used by the agents |
| `GOOGLE_GENAI_USE_VERTEXAI` | Yes | Keep as `FALSE` to use the Gemini API key |
| `GCS_BUCKET` | Yes | Name of the Cloud Storage bucket to write to |
| `GOOGLE_CLOUD_PROJECT` | Recommended | Google Cloud project ID that owns the bucket |
| `PIPELINE_MODEL` | No | Gemini model for the agents (default `gemini-2.5-flash`) |

To analyze different cities, edit the `CITIES` dictionary at the top of
`pipeline_agent/tools.py`.

## Security

No credentials are stored in this repository. The API key and bucket name are
read from a local `.env` file, which is excluded by `.gitignore`. Cloud Storage
access uses Google's Application Default Credentials. The bucket stays private:
the report link only opens for accounts with access to the project.
