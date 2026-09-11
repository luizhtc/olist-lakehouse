from pyspark.sql import DataFrame


def write_managed_table(
    df: DataFrame,
    mode: str,
    table_full_path: str,
    options: dict | None = None,
) -> None:
    writer = df.write.mode(mode)

    if options:
        writer = writer.options(**options)

    writer.saveAsTable(table_full_path)
