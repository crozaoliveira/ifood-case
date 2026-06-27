from pathlib import Path

from config import (
    AWS_REGION,
    BRONZE_PATH,
    LANDING_PATH,
    RUN_ID,
    S3_BASE_PREFIX,
    S3_BUCKET,
    STORAGE_MODE,
)
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name, lit


def to_s3a_path(path: str) -> str:
    if path.startswith("s3://"):
        return f"s3a://{path.removeprefix('s3://')}"

    return path


def get_spark_session() -> SparkSession:
    builder = (
        SparkSession.builder.appName("ifood-case-bronze")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    )
    
    if STORAGE_MODE == 'cloud':
        builder = (
            builder.config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
            )
            .config("spark.hadoop.fs.s3a.endpoint.region", AWS_REGION)
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config(
                "spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:3.4.1,com.amazonaws:aws-java-sdk-bundle:1.12.782",
            )
        )

    return builder.getOrCreate()


def build_s3_base_path() -> str:
    if not S3_BUCKET:
        raise ValueError("S3_BUCKET precisa estar definido no arquivo .env")

    if S3_BASE_PREFIX:
        return f"s3://{S3_BUCKET}/{S3_BASE_PREFIX.strip('/')}"

    return f"s3://{S3_BUCKET}"


def build_landing_run_path() -> str:
    if STORAGE_MODE == "cloud":
        return f"{build_s3_base_path()}/landing/yellow_taxi/{RUN_ID}"

    return f"{LANDING_PATH.rstrip('/')}/{RUN_ID}"


def build_bronze_path() -> str:
    if STORAGE_MODE == "cloud":
        return f"{build_s3_base_path()}/bronze/yellow_taxi"

    return BRONZE_PATH


def validate_local_landing_path(landing_run_path: str) -> None:
    path = Path(landing_run_path)

    if not path.exists():
        raise FileNotFoundError(
            f"Landing path nao encontrado para o RUN_ID={RUN_ID}: {landing_run_path}"
        )

    parquet_files = list(path.glob("*.parquet"))

    if not parquet_files:
        raise FileNotFoundError(
            f"Nenhum arquivo parquet encontrado para o RUN_ID={RUN_ID}: "
            f"{landing_run_path}"
        )


def create_bronze() -> None:
    landing_run_path = build_landing_run_path()
    bronze_path = build_bronze_path()
    source_path = f"{landing_run_path.rstrip('/')}/*.parquet"
    spark_source_path = to_s3a_path(source_path)
    spark_bronze_path = to_s3a_path(bronze_path)

    print("=" * 80)
    print("Iniciando criacao da camada Bronze")
    print(f"Run ID: {RUN_ID}")
    print(f"Storage mode: {STORAGE_MODE}")
    print(f"Origem: {source_path}")
    print(f"Destino: {bronze_path}")
    print("=" * 80)

    if STORAGE_MODE == "local":
        validate_local_landing_path(landing_run_path)

    spark = get_spark_session()

    try:
        bronze_df = (
            spark.read.parquet(spark_source_path)
            .withColumn("run_id", lit(RUN_ID))
            .withColumn("ingestion_timestamp", current_timestamp())
            .withColumn("source_file", input_file_name())
        )

        record_count = bronze_df.count()

        print(f"[INFO] Registros processados: {record_count}")
        print(f"[WRITE] Gravando camada Bronze em: {bronze_path}")

        bronze_df.write.mode("overwrite").parquet(spark_bronze_path)

        print("[OK] Camada Bronze criada com sucesso.")
    except Exception as error:
        if STORAGE_MODE == "cloud":
            raise error 
        """
            RuntimeError(
                "Falha ao criar a camada Bronze lendo/escrevendo no S3. "
                "Verifique credenciais AWS e dependencias Hadoop/S3A do Spark."
            ) from error
        """

        raise
    finally:
        spark.stop()

    print("=" * 80)
    print("Criacao da camada Bronze finalizada.")
    print("=" * 80)


if __name__ == "__main__":
    create_bronze()