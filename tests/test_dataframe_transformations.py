from pyspark.sql.types import StringType

from olist_lakehouse.dataframe_transformations import all_columns_string


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
