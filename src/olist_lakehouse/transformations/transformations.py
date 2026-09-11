from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import coalesce, col, lit, try_to_timestamp


def parse_timestamp(column: Column) -> Column:
    """Parses a string formatted column into a timestamp column given formats

    Args:
        column (Column): The column to be parsed.

    Returns:
        Column: The parsed column with the timestamp information or NULL in case of
        parsing failure.
    """
    FORMATS = ["yyyy-MM-dd HH:mm:ss"]
    return coalesce(*[try_to_timestamp(column, lit(fmt)) for fmt in FORMATS])


def calculate_dimensional_metrics(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn("product_volume_cm3",
            col("product_length_cm")
            * col("product_height_cm")
            * col("product_width_cm"))
        .withColumn("density_g_cm3", col("product_weight_g") / col("product_volume_cm3"))
    )


def all_columns_string(df: DataFrame) -> DataFrame:
    return df.select([col(c).cast("string") for c in df.columns])
