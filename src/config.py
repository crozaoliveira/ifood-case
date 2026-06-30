import os
import argparse
from src.utils import generate_execution_id


catalog_name = "workspace"
schema_name = "ifood_case"
table_name = "ny_tlc_tripdata"

SCHEMA_PATH = f"{catalog_name}.{schema_name}"
VOLUME_PATH = f"/Volumes/{catalog_name}/{schema_name}/{table_name}"

parser = argparse.ArgumentParser()

parser.add_argument("--date-from", required=True)
parser.add_argument("--date-to", required=True)
parser.add_argument("--run-id", required=False)
parser.add_argument("--job-run-id", required=False, default=None)

args = parser.parse_args()

DATE_FROM = args.date_from
DATE_TO = args.date_to

RUN_ID = args.run_id or args.job_run_id

TLC_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

ROOT_PATH = f"{VOLUME_PATH}/{RUN_ID}"
LANDING_PATH = "landing"
BRONZE_PATH = "bronze"
SILVER_PATH = "silver"
