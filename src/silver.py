from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, coalesce
import pyspark.sql.functions as sfn

from src.config import BRONZE_PATH, ROOT_PATH, RUN_ID, SCHEMA_PATH, SILVER_PATH

bronze_table = f"{SCHEMA_PATH}.ny_tlc_bronze"
silver_table = f"{SCHEMA_PATH}.ny_tlc_silver"


def get_spark_session() -> SparkSession:
    return SparkSession.builder.getOrCreate()


def build_bronze_path() -> str:
    return f"{ROOT_PATH}/{BRONZE_PATH}"


def build_silver_path() -> str:
    return f"{ROOT_PATH}/{SILVER_PATH}"


def read_bronze(spark: SparkSession) -> DataFrame:
    """Read Bronze from Unity Catalog, falling back to the run Volume."""
    if spark.catalog.tableExists(bronze_table):
        print(f"[READ TABLE] {bronze_table}")
        return spark.table(bronze_table)

    bronze_path = build_bronze_path()
    print(f"[INFO] Tabela {bronze_table} não encontrada.")
    print(f"[READ PATH] {bronze_path}")
    try:
        return spark.read.parquet(bronze_path)
    except Exception as error:
        raise RuntimeError(
            "Bronze não encontrada no Unity Catalog nem no Volume. "
            f"Tabela: {bronze_table}; caminho: {bronze_path}"
        ) from error


def transform_silver(bronze_df: DataFrame) -> DataFrame:
    """Create the typed and query-ready Yellow Taxi Silver dataset."""
    return (
        bronze_df.select(
            col("VendorID").cast("long").alias("VendorId"),
            col("passenger_count").cast("double").alias("PassengerCount"),
            col("total_amount").cast("decimal(18,2)").alias("TotalAmount"),
            coalesce(
                col("tpep_pickup_datetime"),
                col("lpep_pickup_datetime"),
            ).cast("timestamp").alias("PickupDateTime"),
            coalesce(
                col("tpep_dropoff_datetime"),
                col("lpep_dropoff_datetime"),
            ).cast("timestamp").alias("DropOffDateTime"),
            col("taxi_category").alias("TaxiCategory"),
        )
        .filter(col("PickupDateTime").isNotNull())
        .filter(col("DropOffDateTime").isNotNull())
        .filter(col("DropOffDateTime") >= col("PickupDateTime"))
        .withColumn("PickupYear", sfn.year("PickupDateTime"))
        .withColumn("PickupMonth", sfn.month("PickupDateTime"))
        .withColumn("PickupHour", sfn.hour("PickupDateTime"))
        .withColumn("RunId", sfn.lit(RUN_ID))
        .withColumn("ProcessedAt", sfn.current_timestamp())
    )


def write_silver_parquet(silver_df: DataFrame) -> None:
    silver_path = build_silver_path()
    (
        silver_df.write.mode("overwrite")
        .partitionBy("TaxiCategory", "PickupYear", "PickupMonth")
        .parquet(silver_path)
    )
    print(f"[INFO] Camada Silver criada com sucesso em: {silver_path}")


def write_silver_table(silver_df: DataFrame) -> None:
    (
        silver_df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("PickupYear", "PickupMonth")
        .saveAsTable(silver_table)
    )
    print(f"[INFO] Tabela Silver criada com sucesso em: {silver_table}")


def create_silver(spark: SparkSession | None = None) -> None:
    spark = spark or get_spark_session()

    print(f"[SILVER] Run ID: {RUN_ID}")
    silver_df = transform_silver(read_bronze(spark))
    
    write_silver_parquet(silver_df)
    write_silver_table(silver_df)
    
    print("[OK] Camada Silver criada com sucesso.")


if __name__ == "__main__":
    create_silver()
