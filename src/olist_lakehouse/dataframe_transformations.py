from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import StringType


def all_columns_string(df: DataFrame) -> DataFrame:
    """Casts all DataFrame columns to :class:`StringType` Data Type.

    Args:
        df (:class:`pyspark.sql.DataFrame`): DataFrame to be transformed.

    Returns:
        Transformed PySpark DataFrame with all columns casted as StringType.
    """
    return df.select([col(c).cast(StringType()) for c in df.columns])
