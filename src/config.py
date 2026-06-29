import os
import argparse
from src.utils import generate_execution_id


parser = argparse.ArgumentParser()

parser.add_argument("--date-from", required=True)
parser.add_argument("--date-to", required=True)
parser.add_argument("--volume-path", required=False)
parser.add_argument("--run-id", required=False)

parser.add_argument("--job-run-id", required=False, default=None)
parser.add_argument("--task-run-id", required=False, default=None)
parser.add_argument("--task-key", required=False, default=None)

args = parser.parse_args()

DATE_FROM = args.date_from
DATE_TO = args.date_to
VOLUME_PATH = args.volume_path or "/Volumes/workspace/ifood-case/ny_yellow_taxis"

RUN_ID = args.run_id or args.job_run_id
TASK_RUN_ID = args.task_run_id
TASK_KEY = args.task_key

TLC_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

LANDING_PATH = f"landing/yellow_taxi"
BRONZE_PATH = f"bronze/yellow_taxi"
SILVER_PATH = f"silver/fact_yellow_taxi_trips"
