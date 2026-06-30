from functools import reduce
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, lit

from src.config import BRONZE_PATH, LANDING_PATH, ROOT_PATH, RUN_ID, SCHEMA_PATH


TAXI_CATEGORIES = {
    "yellow": "YELLOW",
    "green": "GREEN",
    "fh": "FH",
    "hvfh": "HVFH",
}

BRONZE_COLUMN_TYPES = {
    "VendorID": "long",
    "passenger_count": "double",
    "trip_distance": "double",
    "RatecodeID": "double",
    "PULocationID": "long",
    "DOLocationID": "long",
    "payment_type": "long",
    "fare_amount": "double",
    "extra": "double",
    "mta_tax": "double",
    "tip_amount": "double",
    "tolls_amount": "double",
    "improvement_surcharge": "double",
    "total_amount": "double",
    "congestion_surcharge": "double",
    "airport_fee": "double",
}

BRONZE_TABLE = f"{SCHEMA_PATH}.ny_tlc_bronze"


def get_spark_session() -> SparkSession:
    return SparkSession.builder.getOrCreate()


def get_taxi_category(dataset: str) -> str:
    try:
        return TAXI_CATEGORIES[dataset]
    except KeyError as error:
        supported = ", ".join(TAXI_CATEGORIES)
        raise ValueError(
            f"Categoria de táxi inválida: {dataset!r}. Categorias suportadas: {supported}."
        ) from error


def build_landing_path(dataset: str) -> str:
    get_taxi_category(dataset)
    return f"{ROOT_PATH}/{LANDING_PATH}/{dataset}"


def build_bronze_path() -> str:
    return f"{ROOT_PATH}/{BRONZE_PATH}"


def list_parquet_files(dataset: str) -> list[str]:
    path = build_landing_path(dataset)
    files = sorted(str(file) for file in Path(path).glob("*.parquet"))

    if not files:
        raise FileNotFoundError(
            f"Nenhum arquivo parquet encontrado para dataset={dataset} em: {path}"
        )

    return files


def standardize_schema(df: DataFrame) -> DataFrame:
    standardized_df = df

    for column_name, data_type in BRONZE_COLUMN_TYPES.items():
        if column_name in standardized_df.columns:
            standardized_df = standardized_df.withColumn(
                column_name,
                col(column_name).cast(data_type),
            )

    return standardized_df


def read_bronze_file(
    spark: SparkSession,
    file_path: str,
    dataset: str,
) -> DataFrame:
    taxi_category = get_taxi_category(dataset)

    return (
        spark.read.parquet(file_path)
        .transform(standardize_schema)
        .withColumn("taxi_category", lit(taxi_category))
        .withColumn("run_id", lit(RUN_ID))
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source_file", lit(file_path))
    )


def create_bronze(spark: SparkSession | None = None) -> None:
    spark = spark or get_spark_session()
    bronze_path = build_bronze_path()
    dataframes: list[DataFrame] = []

    print(f"[BRONZE] Run ID: {RUN_ID}")
    print(f"[WRITE] Path: {bronze_path}")
    print(f"[WRITE] Table: {BRONZE_TABLE}")

    for dataset, taxi_category in TAXI_CATEGORIES.items():
        source_path = build_landing_path(dataset)
        parquet_files = list_parquet_files(dataset)
        print(
            f"[READ] dataset={dataset} taxi_category={taxi_category} "
            f"path={source_path} files={len(parquet_files)}"
        )

        for file_path in parquet_files:
            print(f"[READ FILE] dataset={dataset} file={file_path}")
            try:
                dataframes.append(read_bronze_file(spark, file_path, dataset))
            except Exception as error:
                raise RuntimeError(
                    f"Erro ao processar dataset={dataset} arquivo={file_path}"
                ) from error

    bronze_df = reduce(
        lambda left, right: left.unionByName(right, allowMissingColumns=True),
        dataframes,
    )

    print(f"[INFO] Registros processados: {bronze_df.count()}")

    try:
        bronze_df.write.mode("overwrite").parquet(bronze_path)
        print(f"[OK] Bronze Parquet criada em: {bronze_path}")
    except Exception as error:
        raise RuntimeError(f"Erro ao gravar Bronze em: {bronze_path}") from error

    try:
        (
            bronze_df.write
            .mode("overwrite")
            .format("delta")
            .saveAsTable(BRONZE_TABLE)
        )
        print(f"[OK] Tabela Bronze criada em: {BRONZE_TABLE}")
    except Exception as error:
        raise RuntimeError(f"Erro ao gravar tabela Bronze: {BRONZE_TABLE}") from error

    print("[OK] Camada Bronze criada com sucesso.")


if __name__ == "__main__":
    create_bronze()
