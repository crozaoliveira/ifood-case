from functools import reduce
from pathlib import Path

from src.config import (
    VOLUME_PATH, RUN_ID, LANDING_PATH, BRONZE_PATH
)

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import current_timestamp, lit, col


def get_spark_session() -> SparkSession:
    """Reutiliza a sessão Spark gerenciada pelo Databricks."""
    return (
        SparkSession.builder.getOrCreate()
    )


def build_landing_path() -> str:
    return f"{VOLUME_PATH}/{LANDING_PATH}/{RUN_ID}"


def build_bronze_path() -> str:
    return f"{VOLUME_PATH}/{BRONZE_PATH}"


def list_parquet_files(path: str) -> list[str]:
    files = [str(file) for file in Path(path).glob("*.parquet")]

    if not files:
        raise FileNotFoundError(f"Nenhum arquivo parquet encontrado em: {path}")

    return files


def standardize_schema(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn("VendorID", col("VendorID").cast("long"))
        .withColumn("passenger_count", col("passenger_count").cast("double"))
        .withColumn("trip_distance", col("trip_distance").cast("double"))
        .withColumn("RatecodeID", col("RatecodeID").cast("double"))
        .withColumn("PULocationID", col("PULocationID").cast("long"))
        .withColumn("DOLocationID", col("DOLocationID").cast("long"))
        .withColumn("payment_type", col("payment_type").cast("long"))
        .withColumn("fare_amount", col("fare_amount").cast("double"))
        .withColumn("extra", col("extra").cast("double"))
        .withColumn("mta_tax", col("mta_tax").cast("double"))
        .withColumn("tip_amount", col("tip_amount").cast("double"))
        .withColumn("tolls_amount", col("tolls_amount").cast("double"))
        .withColumn("improvement_surcharge", col("improvement_surcharge").cast("double"))
        .withColumn("total_amount", col("total_amount").cast("double"))
        .withColumn("congestion_surcharge", col("congestion_surcharge").cast("double"))
        .withColumn("airport_fee", col("airport_fee").cast("double"))
    )


def create_bronze(spark: SparkSession | None = None) -> None:
    source_path = build_landing_path()
    bronze_path = build_bronze_path()
    print(f"[BRONZE] Run ID: {RUN_ID}")
    print(f"[READ] {source_path}")
    print(f"[WRITE] {bronze_path}")

    parquet_files = list_parquet_files(source_path)

    spark = get_spark_session()
    dfs = []

    for file_path in parquet_files:
        print(f"[READ FILE] {file_path}")

        df = (
            spark.read.parquet(file_path)
            .transform(standardize_schema)
            .withColumn("run_id", lit(RUN_ID))
            .withColumn("ingestion_timestamp", current_timestamp())
            .withColumn("source_file", lit(file_path))
        )

        dfs.append(df)

    bronze_df = reduce(
        lambda df1, df2: df1.unionByName(df2, allowMissingColumns=True),
        dfs
    )

    print(f"[INFO] Registros processados: {bronze_df.count()}")
    bronze_df.write.mode("overwrite").parquet(bronze_path)
    print("[OK] Camada Bronze criada com sucesso.")

"""
Mesmo depois de salvar o parquet bronze, considerar criar uma tabela bronze em um schema ny_yellow_lakehouse.
"""

if __name__ == "__main__":
    create_bronze()
