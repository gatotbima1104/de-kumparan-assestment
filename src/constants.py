import os
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
ENV_FILE = PROJECT_ROOT / ".env"

loaded = load_dotenv(ENV_FILE, override=False)
print(f"Loading .env from: {ENV_FILE}")


def required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")

    return value


PROJECT_ID = required_env("PROJECT_ID")
REGION = required_env("REGION")

BQ_DATASET_RAW = required_env("BQ_DATASET_RAW")
BQ_DATASET_STAGING = required_env("BQ_DATASET_STAGING")
BQ_DATASET_INTERMEDIATE = required_env("BQ_DATASET_INTERMEDIATE")
BQ_DATASET_MART = required_env("BQ_DATASET_MART")
BQ_DATASET_QUARANTINE = required_env("BQ_DATASET_QUARANTINE")

POSTGRES_HOST = required_env("POSTGRES_HOST")
POSTGRES_PORT = required_env("POSTGRES_PORT")
POSTGRES_DB = required_env("POSTGRES_DB")
POSTGRES_USER = required_env("POSTGRES_USER")
POSTGRES_PASSWORD = required_env("POSTGRES_PASSWORD")
