import tempfile
from pathlib import Path
from typing import Iterable

import boto3
import requests
from botocore.exceptions import ClientError

try:
    from .config import (
        AWS_REGION, DATE_FROM, DATE_TO, RUN_ID, S3_BASE_PREFIX, S3_BUCKET,
        TLC_BASE_URL,
    )
    from .utils import generate_year_month_range
except ImportError:
    from config import (
        AWS_REGION, DATE_FROM, DATE_TO, RUN_ID, S3_BASE_PREFIX, S3_BUCKET,
        TLC_BASE_URL,
    )
    from utils import generate_year_month_range


def build_file_name(year: int, month: int) -> str:
    return f"yellow_tripdata_{year}-{month:02d}.parquet"


def build_file_url(year: int, month: int) -> str:
    return f"{TLC_BASE_URL}/{build_file_name(year, month)}"


def build_landing_relative_key(year: int, month: int) -> str:
    return f"landing/yellow_taxi/{RUN_ID}/{build_file_name(year, month)}"


def build_s3_key(year: int, month: int) -> str:
    relative_key = build_landing_relative_key(year, month)
    return f"{S3_BASE_PREFIX}/{relative_key}" if S3_BASE_PREFIX else relative_key


def get_s3_client():
    return boto3.client("s3", region_name=AWS_REGION)


def s3_object_exists(bucket: str, key: str) -> bool:
    try:
        get_s3_client().head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as error:
        status_code = error.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        error_code = error.response.get("Error", {}).get("Code")
        if status_code == 404 or error_code in {"404", "NoSuchKey", "NotFound"}:
            return False
        raise


def download_file(url: str, destination_path: Path) -> None:
    with requests.get(url, stream=True, timeout=120) as response:
        response.raise_for_status()
        with destination_path.open("wb") as file:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    file.write(chunk)


def ingest_file(year: int, month: int) -> None:
    url = build_file_url(year, month)
    s3_key = build_s3_key(year, month)
    s3_uri = f"s3://{S3_BUCKET}/{s3_key}"

    if s3_object_exists(S3_BUCKET, s3_key):
        print(f"[SKIP] Arquivo já existe no S3: {s3_uri}")
        return

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_file_path = Path(temp_dir) / build_file_name(year, month)
        print(f"[DOWNLOAD] {url}")
        download_file(url, temp_file_path)
        print(f"[UPLOAD] {s3_uri}")
        get_s3_client().upload_file(str(temp_file_path), S3_BUCKET, s3_key)

    print(f"[OK] Arquivo enviado para o S3: {s3_uri}")


def ingest_months(periods: Iterable[tuple[int, int]]) -> None:
    print("=" * 80)
    print("Iniciando ingestão da landing zone no S3")
    print(f"Run ID: {RUN_ID}")
    print(f"Período: {DATE_FROM} a {DATE_TO}")
    print("=" * 80)

    for year, month in periods:
        ingest_file(year, month)

    print("[OK] Ingestão finalizada com sucesso.")


if __name__ == "__main__":
    ingest_months(generate_year_month_range(DATE_FROM, DATE_TO))
