import os
import requests
from pathlib import Path
from typing import Iterable
from src.utils import generate_year_month_range

from src.config import (
    DATE_FROM, DATE_TO, VOLUME_PATH, RUN_ID,
    TLC_BASE_URL, LANDING_PATH
)


def build_file_name(year: int, month: int) -> str:
    return f"yellow_tripdata_{year}-{month:02d}.parquet"


def build_file_url(year: int, month: int) -> str:
    return f"{TLC_BASE_URL}/{build_file_name(year, month)}"


def build_landing_relative_key(year: int, month: int) -> str:
    return f"{VOLUME_PATH}/{LANDING_PATH}/{RUN_ID}/{build_file_name(year, month)}"


def download_file(url: str, destination_path: Path) -> None:
    destination_dir = os.path.dirname(destination_path)
    os.makedirs(destination_dir, exist_ok=True)

    with requests.get(url, stream=True, timeout=120) as response:
        response.raise_for_status()
        with open(destination_path, "wb") as file:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    file.write(chunk)


def ingest_file(year: int, month: int) -> None:
    url = build_file_url(year, month)
    s3_key = build_landing_relative_key(year, month)
    file_path = build_file_name(year, month)
    print(f"[DOWNLOAD] {url}")
    download_file(url, s3_key)
    print(f"[UPLOAD] {s3_key}")

    print(f"[OK] Arquivo enviado para o S3: {s3_key}")


def ingest_months(periods: Iterable[tuple[int, int]]) -> None:
    print("=" * 80)
    print("Iniciando ingestão da landing zone no S3")
    print(f"Job Run ID: {RUN_ID}")
    print(f"Período: {DATE_FROM} a {DATE_TO}")
    print("=" * 80)

    for year, month in periods:
        ingest_file(year, month)

    print("[OK] Ingestão finalizada com sucesso.")


if __name__ == "__main__":
    ingest_months(generate_year_month_range(DATE_FROM, DATE_TO))