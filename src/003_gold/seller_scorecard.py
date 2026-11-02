# Databricks notebook source
# DBTITLE 1,Imports
from pyspark.sql.functions import (
    avg,
    col,
    countDistinct,
    lit,
    when,
)
from pyspark.sql.functions import (
    round as _round,
)
from pyspark.sql.functions import (
    sum as _sum,
)

# COMMAND ----------

# DBTITLE 1,Define parameters
dbutils.widgets.text("catalog_name", "")
dbutils.widgets.text("silver_schema_name", "")
dbutils.widgets.text("gold_schema_name", "")

catalog_name = dbutils.widgets.get("catalog_name")
silver_schema_name = dbutils.widgets.get("silver_schema_name")
gold_schema_name = dbutils.widgets.get("gold_schema_name")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Seller scorecard table
# MAGIC Which sellers have the best combination of average review score and on time delivery? Who are the top performers?
# MAGIC
# MAGIC **Table requirements:**
# MAGIC
# MAGIC - Grouped by seller_id
# MAGIC - Filter only sellers with at least 20 delivered orders
# MAGIC
# MAGIC | Metric             | Description                                                                                                             |
# MAGIC | -------------------| ------------------------------------------------------------------------------------------------------------------------|
# MAGIC | `seller_id`        | Seller ID                                                                                                               |
# MAGIC | `seller_state`     | Seller state                                                                                                            |
# MAGIC | `total_orders`     | Total seller orders                                                                                                     |
# MAGIC | `total_revenue`    | Total seller revenue                                                                                                    |
# MAGIC | `avg_review_score` | Average review score                                                                                                    |
# MAGIC | `on_time_rate`     | On time delivery rate                                                                                                   |
# MAGIC | `late_order_pct`   | 1 - `on_time_rate`                                                                                                      |
# MAGIC | `seller_tier`      | `'top'` if score ≥ 4.5 and `on_time_rate` ≥ 0.90; `'at risk'` if score < 3.5 or `on_time_rate` < 0.70; `'regular'` else |

# COMMAND ----------

# DBTITLE 1,Read
orders = spark.read.table(f"{catalog_name}.{silver_schema_name}.orders")
order_items = spark.read.table(f"{catalog_name}.{silver_schema_name}.order_items")
reviews = spark.read.table(f"{catalog_name}.{silver_schema_name}.reviews")

# COMMAND ----------

# DBTITLE 1,Filter valid sellers
valid_sellers = (
    orders.filter(col("order_status") == "delivered")
    .join(order_items, on="order_id", how="left")
    .groupBy("seller_id", "seller_state")
    .agg(countDistinct(col("order_id")).alias("total_delivered_orders"))
    .filter(col("total_delivered_orders") >= 20)
    .select("seller_id", "seller_state")
)

# COMMAND ----------

# DBTITLE 1,Join
order_items_valid_sellers = (
    order_items.join(valid_sellers, on="seller_id", how="inner")
    .select("order_id", "seller_id")
    .distinct()
)

# COMMAND ----------

# DBTITLE 1,Calculate time metrics
seller_time = (
    orders.join(order_items_valid_sellers, on="order_id", how="inner")
    .groupBy("seller_id")
    .agg(
        _round(
            _sum(when(~col("is_late"), 1).otherwise(0))
            / countDistinct(col("order_id")),
            2,
        ).alias("on_time_rate")
    )
    .withColumn("late_order_pct", _round(1 - col("on_time_rate"), 2))
)

# COMMAND ----------

# DBTITLE 1,Calculate seller earnings
seller_earnings = (
    order_items_valid_sellers.join(
        order_items, on=["order_id", "seller_id"], how="inner"
    )
    .groupBy(col("seller_id"))
    .agg(
        countDistinct(col("order_id")).alias("total_orders"),
        _round(_sum(col("price")), 2).alias("total_revenue"),
    )
)

# COMMAND ----------

# DBTITLE 1,Calculate reviews
seller_reviews = (
    order_items_valid_sellers.join(reviews, on="order_id", how="inner")
    .groupBy("seller_id")
    .agg(_round(avg(col("review_score")), 2).alias("avg_review_score"))
)

# COMMAND ----------

# DBTITLE 1,Join everything together
seller_scorecard = (
    valid_sellers.join(seller_earnings, on="seller_id", how="inner")
    .join(seller_reviews, on="seller_id", how="inner")
    .join(seller_time, on="seller_id", how="inner")
    .withColumn(
        "seller_tier",
        when((col("avg_review_score") >= 4.5) & (col("on_time_rate") >= 0.9), "top")
        .when((col("avg_review_score") < 3.5) | (col("on_time_rate") < 0.7), "at risk")
        .otherwise(lit("regular")),
    )
)

# COMMAND ----------

# DBTITLE 1,Write
target_tbl = f"{catalog_name}.{gold_schema_name}.seller_scorecard"
(
    seller_scorecard.write.mode("overwrite")
    .options(overwriteSchema="true")
    .saveAsTable(target_tbl)
)
