import os
from dotenv import load_dotenv
from utils import generate_execution_id


load_dotenv()

def set_run_id():
    run_id = os.getenv("RUN_ID")
    if not run_id:
        run_id = generate_execution_id()
        os.environ["RUN_ID"] = run_id
    return run_id

RUN_ID = set_run_id()

STORAGE_MODE = os.getenv("STORAGE_MODE", "local").lower()

LOCAL_BASE_PATH = os.getenv("LOCAL_BASE_PATH", "data")

AWS_REGION = os.getenv("AWS_REGION", "us-east-2")
S3_BUCKET = os.getenv("S3_BUCKET")
S3_BASE_PREFIX = os.getenv("S3_BASE_PREFIX", "").strip("/")

DATE_FROM = os.getenv("DATE_FROM", None)
DATE_TO = os.getenv("DATE_TO", None)

if DATE_FROM is None or DATE_TO is None:
    raise ValueError("As variáveis de ambiente DATE_FROM e DATE_TO devem estar definidas no arquivo .env")

TLC_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

def get_base_path() -> str:
    if STORAGE_MODE == "cloud":
        if not S3_BUCKET:
            raise ValueError("S3_BUCKET precisa estar definido no arquivo .env")

        if S3_BASE_PREFIX:
            return f"s3://{S3_BUCKET}/{S3_BASE_PREFIX}"

        return f"s3://{S3_BUCKET}"

    return LOCAL_BASE_PATH


BASE_PATH = get_base_path()

LANDING_PATH = f"{BASE_PATH}/landing/yellow_taxi"
BRONZE_PATH = f"{BASE_PATH}/bronze/yellow_taxi"
SILVER_PATH = f"{BASE_PATH}/silver/fact_yellow_taxi_trips"