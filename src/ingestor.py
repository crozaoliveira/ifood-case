import os
from pathlib import Path
from typing import Iterable

import requests

from src.config import DATE_FROM, DATE_TO, LANDING_PATH, ROOT_PATH, RUN_ID, TLC_BASE_URL
from src.utils import generate_year_month_range


DATASET_FILE_PREFIXES = {
    "yellow": "yellow",
    "green": "green",
    "fh": "fhv",
    "hvfh": "fhvhv",
}
DATASET_ALIASES = tuple(DATASET_FILE_PREFIXES)


def get_file_prefix(dataset: str) -> str:
    try:
        return DATASET_FILE_PREFIXES[dataset]
    except KeyError as error:
        supported = ", ".join(DATASET_ALIASES)
        raise ValueError(
            f"Tipo de dataset inválido: {dataset!r}. Tipos suportados: {supported}."
        ) from error


def build_file_name(dataset: str, year: int, month: int) -> str:
    return f"{get_file_prefix(dataset)}_tripdata_{year}-{month:02d}.parquet"


def build_file_url(dataset: str, year: int, month: int) -> str:
    return f"{TLC_BASE_URL}/{build_file_name(dataset, year, month)}"


def build_dataset_landing_path(dataset: str) -> str:
    get_file_prefix(dataset)
    return f"{ROOT_PATH}/{LANDING_PATH}/{dataset}"


def build_landing_relative_key(dataset: str, year: int, month: int) -> str:
    return f"{build_dataset_landing_path(dataset)}/{build_file_name(dataset, year, month)}"


def download_file(url: str, destination_path: Path) -> None:
    destination_dir = os.path.dirname(destination_path)
    os.makedirs(destination_dir, exist_ok=True)

    with requests.get(url, stream=True, timeout=120) as response:
        response.raise_for_status()
        with open(destination_path, "wb") as file:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    file.write(chunk)


def ingest_file(dataset: str, year: int, month: int) -> None:
    url = build_file_url(dataset, year, month)
    destination = Path(build_landing_relative_key(dataset, year, month))
    period = f"{year}-{month:02d}"

    if destination.exists():
        print(f"[SKIP] dataset={dataset} período={period} arquivo={destination}")
        return

    print(f"[DOWNLOAD] dataset={dataset} período={period} url={url}")
    try:
        download_file(url, destination)
    except Exception as error:
        raise RuntimeError(
            f"Falha ao ingerir dataset={dataset} período={period} url={url}"
        ) from error

    print(f"[OK] dataset={dataset} período={period} arquivo={destination}")


def ingest_months(periods: Iterable[tuple[int, int]]) -> None:
    print("=" * 80)
    print("Iniciando ingestão da landing zone TLC")
    print(f"Job Run ID: {RUN_ID}")
    print(f"Período: {DATE_FROM} a {DATE_TO}")
    print(f"Datasets: {', '.join(DATASET_ALIASES)}")
    print("=" * 80)

    for year, month in periods:
        for dataset in DATASET_ALIASES:
            ingest_file(dataset, year, month)

    print("[OK] Ingestão finalizada com sucesso.")


if __name__ == "__main__":
    ingest_months(generate_year_month_range(DATE_FROM, DATE_TO))
