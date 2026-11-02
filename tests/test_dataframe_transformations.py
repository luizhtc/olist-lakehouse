from pyspark.sql.functions import col
from pyspark.sql.types import StringType, TimestampType

from olist_lakehouse.dataframe_transformations import (
    all_columns_string,
    parse_timestamp,
)


def test_all_columns_string_casts_every_column_to_string(spark):
    df = spark.createDataFrame(
        [(1, 2.5, True, "abc")],
        ["int_col", "float_col", "bool_col", "str_col"],
    )

    result = all_columns_string(df)

    assert result.columns == ["int_col", "float_col", "bool_col", "str_col"]
    assert all(field.dataType == StringType() for field in result.schema.fields)
    assert result.collect()[0].asDict() == {
        "int_col": "1",
        "float_col": "2.5",
        "bool_col": "true",
        "str_col": "abc",
    }


def test_parse_timestamp_parses_valid_values(spark):
    df = spark.createDataFrame(
        [("2024-01-15 12:30:45",)],
        ["ts_col"],
    )

    result = df.select(parse_timestamp(col("ts_col")).alias("parsed_ts"))
    row = result.collect()[0]

    assert row["parsed_ts"].strftime("%Y-%m-%d %H:%M:%S") == "2024-01-15 12:30:45"


def test_parse_timestamp_returns_null_for_invalid(spark):
    df = spark.createDataFrame(
        [("not-a-timestamp",)],
        ["ts_col"],
    )

    result = df.select(parse_timestamp(col("ts_col")).alias("parsed_ts"))
    row = result.collect()[0]

    assert row["parsed_ts"] is None


def test_parse_timestamp_returns_timestamp_type(spark):
    df = spark.createDataFrame(
        [("2024-01-15 12:30:45",)],
        ["ts_col"],
    )

    result = df.select(parse_timestamp(col("ts_col")).alias("parsed_ts"))

    assert result.schema["parsed_ts"].dataType == TimestampType()
