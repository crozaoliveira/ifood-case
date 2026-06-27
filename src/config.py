import os

from dotenv import load_dotenv

try:
    from .utils import generate_execution_id
except ImportError:
    from utils import generate_execution_id

load_dotenv()


def require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"A variável de ambiente {name} deve estar definida")
    return value


RUN_ID = os.getenv("RUN_ID") or generate_execution_id()
os.environ["RUN_ID"] = RUN_ID

AWS_REGION = os.getenv("AWS_REGION", "us-east-2")
S3_BUCKET = require_env("S3_BUCKET")
S3_BASE_PREFIX = os.getenv("S3_BASE_PREFIX", "").strip("/")

DATE_FROM = require_env("DATE_FROM")
DATE_TO = require_env("DATE_TO")

TLC_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

S3_BASE_PATH = f"s3://{S3_BUCKET}"
if S3_BASE_PREFIX:
    S3_BASE_PATH = f"{S3_BASE_PATH}/{S3_BASE_PREFIX}"

LANDING_PATH = f"{S3_BASE_PATH}/landing/yellow_taxi"
BRONZE_PATH = f"{S3_BASE_PATH}/bronze/yellow_taxi"
SILVER_PATH = f"{S3_BASE_PATH}/silver/fact_yellow_taxi_trips"
