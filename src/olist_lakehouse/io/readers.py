from pyspark.sql import SparkSession


def read_managed_table(catalog: str, schema: str, table: str):
    spark = SparkSession.getActiveSession()
    if spark is None:
        raise ValueError("No active SparkSession found.")

    return spark.read.table(
        f"{catalog}.{schema}.{table}"
    )
