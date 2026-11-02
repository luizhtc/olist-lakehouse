# Databricks notebook source
from pyspark.sql.functions import (
    avg,
    col,
    countDistinct,
    when,
)
from pyspark.sql.functions import (
    round as _round,
)
from pyspark.sql.functions import (
    sum as _sum,
)

# COMMAND ----------

dbutils.widgets.text("catalog_name", "")
dbutils.widgets.text("silver_schema_name", "")
dbutils.widgets.text("gold_schema_name", "")

catalog_name = dbutils.widgets.get("catalog_name")
silver_schema_name = dbutils.widgets.get("silver_schema_name")
gold_schema_name = dbutils.widgets.get("gold_schema_name")

# COMMAND ----------

orders = spark.read.table(f"{catalog_name}.{silver_schema_name}.orders")
orders_not_null = orders.filter(col("delivery_delay_days").isNotNull())

reviews = spark.read.table(f"{catalog_name}.{silver_schema_name}.reviews")

orders_reviews = orders_not_null.join(
    reviews,
    on="order_id",
    how="inner",
)

# COMMAND ----------

delay_x_customer_satisfaction = (
    orders_reviews.withColumn(
        "delay_range",
        when(col("delivery_delay_days") < 0, "Early")
        .when(col("delivery_delay_days") == 0, "On time")
        .when(col("delivery_delay_days").between(1, 3), "1 to 3 days")
        .when(col("delivery_delay_days").between(4, 7), "4 to 7 days")
        .when(col("delivery_delay_days") > 7, "More than 7 days"),
    )
    .groupBy("delay_range")
    .agg(
        countDistinct(col("order_id")).alias("total_orders"),
        _round(avg(col("review_score")), 2).alias("avg_review_score"),
        _round(
            (
                _sum(when(col("review_score") <= 2, 1).otherwise(0))
                / countDistinct(col("order_id"))
                * 100
            ),
            2,
        ).alias("pct_negative_reviews"),
    )
)

# COMMAND ----------

target_tbl = f"{catalog_name}.{gold_schema_name}.delay_x_customer_satisfaction"
(
    delay_x_customer_satisfaction.write.mode("overwrite")
    .options(overwriteSchema="true")
    .saveAsTable(target_tbl)
)
