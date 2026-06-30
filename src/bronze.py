from functools import reduce
from pathlib import Path

from src.config import (
    RUN_ID, 
    SCHEMA_PATH, ROOT_PATH, 
    LANDING_PATH, BRONZE_PATH
)

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import current_timestamp, lit, col


def get_spark_session() -> SparkSession:
    return (
        SparkSession.builder.getOrCreate()
    )


def build_landing_path() -> str:
    return f"{ROOT_PATH}/{LANDING_PATH}"


def build_bronze_path() -> str:
    return f"{ROOT_PATH}/{BRONZE_PATH}"


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

        try: 
            df = (
                spark.read.parquet(file_path)
                .transform(standardize_schema)
                .withColumn("run_id", lit(RUN_ID))
                .withColumn("ingestion_timestamp", current_timestamp())
                .withColumn("source_file", lit(file_path))
            )

            dfs.append(df)
        except Exception as e:
            raise Exception(f"[WARN] Erro ao processar arquivo: {file_path} - {e}")

    bronze_df = reduce(
        lambda df1, df2: df1.unionByName(df2, allowMissingColumns=True),
        dfs
    )

    print(f"[INFO] Registros processados: {bronze_df.count()}")

    try:
        bronze_df.write.mode("overwrite").parquet(bronze_path)
        print(f"[INFO] Camada Bronze criada com sucesso em: {bronze_path}")
    except Exception as e:
        raise Exception(f"[WARN] Erro ao criar camada Bronze: {e}")

    try:
        bronze_table = f"{SCHEMA_PATH}.ny_yellow_bronze"
        (
            bronze_df.write
            .mode("overwrite")
            .format("delta")
            .saveAsTable(bronze_table)
        )
        print(f"[INFO] Tabela Bronze criada com sucesso em: {bronze_table}")
    except Exception as e:
        raise Exception(f"[WARN] Erro ao criar tabela Bronze: {e}")

    print("[OK] Camada Bronze criada com sucesso.")

if __name__ == "__main__":
    create_bronze()
