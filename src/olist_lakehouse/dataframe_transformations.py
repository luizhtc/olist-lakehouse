from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import coalesce, col, lit, try_to_timestamp
from pyspark.sql.types import StringType


def all_columns_string(df: DataFrame) -> DataFrame:
    """Casts all DataFrame columns to :class:`StringType` Data Type.

    Args:
        df (:class:`pyspark.sql.DataFrame`): DataFrame to be transformed.

    Returns:
        Transformed PySpark DataFrame with all columns casted as StringType.
    """
    return df.select([col(c).cast(StringType()) for c in df.columns])


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
