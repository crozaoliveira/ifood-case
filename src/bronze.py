try:
    from .config import BRONZE_PATH, LANDING_PATH, RUN_ID
except ImportError:
    from config import BRONZE_PATH, LANDING_PATH, RUN_ID

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name, lit


def get_spark_session() -> SparkSession:
    """Reutiliza a sessão Spark gerenciada pelo Databricks."""
    return (
        SparkSession.builder.appName("ifood-case-bronze")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )


def build_landing_run_path() -> str:
    return f"{LANDING_PATH}/{RUN_ID}"


def create_bronze(spark: SparkSession | None = None) -> None:
    source_path = f"{build_landing_run_path()}/*.parquet"
    print(f"[BRONZE] Run ID: {RUN_ID}")
    print(f"[READ] {source_path}")
    print(f"[WRITE] {BRONZE_PATH}")

    spark = spark or get_spark_session()
    bronze_df = (
        spark.read.parquet(source_path)
        .withColumn("run_id", lit(RUN_ID))
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source_file", input_file_name())
    )

    print(f"[INFO] Registros processados: {bronze_df.count()}")
    bronze_df.write.mode("overwrite").parquet(BRONZE_PATH)
    print("[OK] Camada Bronze criada com sucesso.")


if __name__ == "__main__":
    create_bronze()
