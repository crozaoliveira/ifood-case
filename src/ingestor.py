import os
import tempfile

from config import (
    AWS_REGION,
    RUN_ID,
    LANDING_PATH,
    S3_BUCKET,
    S3_BASE_PREFIX,
    STORAGE_MODE,
    TLC_BASE_URL,
    DATE_FROM,
    DATE_TO,
)

from pathlib import Path
from typing import Iterable

import boto3
import requests
from botocore.exceptions import ClientError
from utils import generate_year_month_range


def build_file_name(year: int, month: int) -> str:
    return f"yellow_tripdata_{year}-{month:02d}.parquet"


def build_file_url(year: int, month: int) -> str:
    file_name = build_file_name(year, month)
    return f"{TLC_BASE_URL}/{file_name}"


def build_landing_relative_key(year: int, month: int) -> str:
    file_name = build_file_name(year, month)
    return f"landing/yellow_taxi/{RUN_ID}/{file_name}"


def build_local_landing_path(year: int, month: int) -> Path:
    file_name = build_file_name(year, month)
    return Path(LANDING_PATH) / RUN_ID / file_name


def build_cloud_key(year: int, month: int) -> str:
    relative_key = build_landing_relative_key(year, month)

    if S3_BASE_PREFIX:
        return f"{S3_BASE_PREFIX.strip('/')}/{relative_key}"

    return relative_key


def get_cloud_client():
    return boto3.client("s3", region_name=AWS_REGION)


def cloud_object_exists(bucket: str, key: str) -> bool:
    cloud_client = get_cloud_client()

    try:
        cloud_client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as error:
        status_code = error.response.get("ResponseMetadata", {}).get("HTTPStatusCode")

        if status_code == 404:
            return False

        raise


def local_file_exists(path: Path) -> bool:
    return path.exists() and path.is_file()


def download_file(url: str, destination_path: Path) -> None:
    destination_path.parent.mkdir(parents=True, exist_ok=True)

    with requests.get(url, stream=True, timeout=120) as response:
        response.raise_for_status()

        with open(destination_path, "wb") as file:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    file.write(chunk)


def upload_file_to_cloud(local_path: Path, bucket: str, key: str) -> None:
    cloud_client = get_cloud_client()
    cloud_client.upload_file(str(local_path), bucket, key)


def ingest_file_to_local(year: int, month: int) -> None:
    url = build_file_url(year, month)
    destination_path = build_local_landing_path(year, month)

    if local_file_exists(destination_path):
        print(f"[SKIP] Arquivo já existe localmente: {destination_path}")
        return

    print(f"[DOWNLOAD] {url}")
    print(f"[LOCAL] Salvando em: {destination_path}")

    download_file(url, destination_path)

    print(f"[OK] Arquivo salvo localmente: {destination_path}")


def ingest_file_to_cloud(year: int, month: int) -> None:
    if not S3_BUCKET:
        raise ValueError("S3_BUCKET precisa estar definido no arquivo .env")

    url = build_file_url(year, month)
    cloud_key = build_cloud_key(year, month)

    if cloud_object_exists(S3_BUCKET, cloud_key):
        print(f"[SKIP] Arquivo já existe no S3: s3://{S3_BUCKET}/{cloud_key}")
        return

    with tempfile.TemporaryDirectory() as temp_dir:
        file_name = build_file_name(year, month)
        temp_file_path = Path(temp_dir) / file_name

        print(f"[DOWNLOAD] {url}")
        print(f"[TEMP] Salvando temporariamente em: {temp_file_path}")

        download_file(url, temp_file_path)

        print(f"[UPLOAD] Enviando para: s3://{S3_BUCKET}/{cloud_key}")

        upload_file_to_cloud(temp_file_path, S3_BUCKET, cloud_key)

    print(f"[OK] Arquivo enviado para S3: s3://{S3_BUCKET}/{cloud_key}")


def ingestor(year: int, month: int) -> None:
    if STORAGE_MODE == "cloud":
        ingest_file_to_cloud(year, month)
    else:
        ingest_file_to_local(year, month)


def ingest_months(periods: Iterable[tuple[int, int]]) -> None:
    print("=" * 80)
    print("Iniciando ingestão da landing zone")
    print(f"Run ID: {RUN_ID}")
    print(f"Storage mode: {STORAGE_MODE}")
    print(f"Data de início: {DATE_FROM}")
    print(f"Data de término: {DATE_TO}")
    print("=" * 80)

    for year, month in periods:
        ingestor(year, month)

    print("=" * 80)
    print("Ingestão finalizada com sucesso.")
    print("=" * 80)


if __name__ == "__main__":
    periods = generate_year_month_range(DATE_FROM, DATE_TO)
    ingest_months(periods)