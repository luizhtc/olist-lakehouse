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

orders = spark.read.table("cat_olist.sch_silver.orders").filter(
    col("delivery_delay_days").isNotNull()
)

# COMMAND ----------

reviews = spark.read.table("cat_olist.sch_silver.reviews")

# COMMAND ----------

orders_reviews = orders.join(
    reviews,
    on="order_id",
    how="inner",
)

# COMMAND ----------

orders_reviews.withColumn(
    "delay_range",
    when(col("delivery_delay_days") < 0, "Early")
    .when(col("delivery_delay_days") == 0, "On time")
    .when(col("delivery_delay_days").between(1, 3), "1 to 3 days")
    .when(col("delivery_delay_days").between(4, 7), "4 to 7 days")
    .when(col("delivery_delay_days") > 7, "More than 7 days"),
).groupBy("delay_range").agg(
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
).display()

