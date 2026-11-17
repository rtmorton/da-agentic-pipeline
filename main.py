"""Runs the weather data pipeline from the command line.

Usage:
    python main.py
"""

import asyncio
import os
import sys

from google.adk.runners import InMemoryRunner
from google.genai import types

from pipeline_agent.agent import root_agent

APP_NAME = "weather_pipeline"
USER_ID = "student"
REQUIRED_SETTINGS = ("GOOGLE_API_KEY", "GCS_BUCKET")


def check_settings() -> None:
  missing = [name for name in REQUIRED_SETTINGS if not os.getenv(name)]
  if missing:
    print(
        "Missing settings: " + ", ".join(missing) + "\n"
        "Copy env.example to .env and fill in the values (see README.md)."
    )
    sys.exit(1)


async def run_pipeline() -> None:
  runner = InMemoryRunner(agent=root_agent, app_name=APP_NAME)
  session = await runner.session_service.create_session(
      app_name=APP_NAME, user_id=USER_ID
  )
  message = types.Content(
      role="user", parts=[types.Part(text="Run the weather data pipeline.")]
  )

  print("Starting pipeline...\n")
  async for event in runner.run_async(
      user_id=USER_ID, session_id=session.id, new_message=message
  ):
    if not event.content or not event.content.parts:
      continue
    for part in event.content.parts:
      if part.function_call:
        print(f"[{event.author}] calling tool: {part.function_call.name}")
      elif part.text and part.text.strip():
        print(f"[{event.author}] {part.text.strip()}\n")

  print("Pipeline finished. Local outputs are in the output/ folder.")


if __name__ == "__main__":
  check_settings()
  asyncio.run(run_pipeline())
