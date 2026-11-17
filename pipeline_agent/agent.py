"""Agent definitions for the weather data pipeline.

The pipeline is orchestrated by a SequentialAgent that runs four
specialist LlmAgents in order. Each specialist owns one stage and calls the
matching tool from ``tools.py``:

    weather_pipeline (SequentialAgent)
      1. ingestion_agent       -> fetch_weather_data
      2. processing_agent      -> transform_weather_data
      3. storage_agent         -> upload_to_gcs
      4. visualization_agent   -> build_html_report

Data moves between stages through the shared session state, and each
agent's short text summary is saved to the state with ``output_key``.
"""

import os
from pathlib import Path

from dotenv import load_dotenv

# Load settings (project, bucket, model access) from the .env file in the
# project root. override=True makes the .env values win over variables the
# environment already sets, such as the default project in Cloud Shell.
load_dotenv(Path(__file__).resolve().parent.parent / ".env", override=True)

from google.adk.agents import LlmAgent  # noqa: E402
from google.adk.agents import SequentialAgent  # noqa: E402

from .tools import build_html_report  # noqa: E402
from .tools import fetch_weather_data  # noqa: E402
from .tools import transform_weather_data  # noqa: E402
from .tools import upload_to_gcs  # noqa: E402

MODEL = os.getenv("PIPELINE_MODEL", "gemini-3.5-flash")

_RULES = (
    "Call your tool exactly once. Do not invent numbers: only report values "
    "the tool returned. If the tool returns status 'error', state the error "
    "message clearly and say that the pipeline cannot continue."
)

ingestion_agent = LlmAgent(
    name="ingestion_agent",
    model=MODEL,
    description="Pulls raw daily weather data from the Open-Meteo public API.",
    instruction=(
        "You are the ingestion step of a data pipeline. Call "
        "fetch_weather_data, then reply in one sentence with how many cities "
        "and days were fetched. " + _RULES
    ),
    tools=[fetch_weather_data],
    output_key="ingestion_result",
)

processing_agent = LlmAgent(
    name="processing_agent",
    model=MODEL,
    description="Cleans, filters, groups, and aggregates the raw weather data.",
    instruction=(
        "You are the processing step of a data pipeline. Call "
        "transform_weather_data, then "
        "summarize the results in two or three sentences: the time period, "
        "the warmest and wettest city, and how many rows were kept and "
        "dropped. " + _RULES
    ),
    tools=[transform_weather_data],
    output_key="processing_result",
)

storage_agent = LlmAgent(
    name="storage_agent",
    model=MODEL,
    description="Saves the raw and processed data to Google Cloud Storage.",
    instruction=(
        "You are the storage step of a data pipeline. Call upload_to_gcs, "
        "then list the gs:// locations that were uploaded. " + _RULES
    ),
    tools=[upload_to_gcs],
    output_key="storage_result",
)

visualization_agent = LlmAgent(
    name="visualization_agent",
    model=MODEL,
    description="Builds the HTML report that visualizes the processed data.",
    instruction=(
        "You are the final visualization step of a data pipeline. Call "
        "build_html_report, then give a short final summary of the whole "
        "pipeline run, including where the report was saved and its browser "
        "link if one was returned. " + _RULES
    ),
    tools=[build_html_report],
    output_key="visualization_result",
)

root_agent = SequentialAgent(
    name="weather_pipeline",
    description=(
        "End-to-end weather data pipeline: ingest, process, store, and "
        "visualize."
    ),
    sub_agents=[
        ingestion_agent,
        processing_agent,
        storage_agent,
        visualization_agent,
    ],
)
