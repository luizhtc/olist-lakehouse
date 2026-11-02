# Databricks notebook source
# DBTITLE 1,Imports
from pyspark.sql import Window
from pyspark.sql.functions import (
    avg,
    col,
    countDistinct,
    rank,
    when,
)
from pyspark.sql.functions import (
    max as _max,
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
# MAGIC ## Delivery performance table
# MAGIC Which states have the worst delivery performance?
# MAGIC Is it correlated to the seller state and delivery delay?
# MAGIC
# MAGIC **Table requirements:**
# MAGIC
# MAGIC - Grouping by the seller state
# MAGIC
# MAGIC | Metric            | Description                                           |
# MAGIC | --------------------| ----------------------------------------------------|
# MAGIC | `seller_state`     | Seller state                                         |
# MAGIC | `total_orders`     | Total delivered orders                               |
# MAGIC | `on_time_orders`   | Orders delivered on time or before                   |
# MAGIC | `late_orders`      | Late delivered orders                                |
# MAGIC | `on_time_rate`     | `on_time_orders` / `total_orders` (`0.00` to `1.00`) |
# MAGIC | `avg_delay_days`   | Average `delivery_delay_days` (including negatives)  |
# MAGIC | `max_delay_days`   | Maximum days of delay                                |
# MAGIC | `performance_rank` | Ranking by `on_time_rate` descending                 |

# COMMAND ----------

# DBTITLE 1,Read
orders = spark.read.table(f"{catalog_name}.{silver_schema_name}.orders")
order_items = spark.read.table(f"{catalog_name}.{silver_schema_name}.order_items")

# COMMAND ----------

# DBTITLE 1,Pre - Group by order_id
pre_group_orders = (
    order_items.join(orders, on="order_id", how="inner")
    .select("order_id", "seller_state", "delivery_delay_days", "is_late")
    .distinct()
)

# COMMAND ----------

# DBTITLE 1,Group by seller state for delivery
delivery_performance = (
    pre_group_orders.groupby("seller_state")
    .agg(
        countDistinct(col("order_id")).alias("total_orders"),
        _sum(when(col("is_late"), 0).otherwise(1)).alias("on_time_orders"),
        _sum(when(col("is_late"), 1).otherwise(0)).alias("late_orders"),
        avg(col("delivery_delay_days")).alias("avg_delay_days"),
        _max(col("delivery_delay_days")).alias("max_delay_days"),
    )
    .withColumn("on_time_rate", _round(col("on_time_orders") / col("total_orders"), 2))
)

# COMMAND ----------

# DBTITLE 1,Window ranking
wn_performance = Window.orderBy(col("on_time_rate").desc())
delivery_performance_ranked = delivery_performance.withColumn(
    "performance_rank", rank().over(wn_performance)
)

# COMMAND ----------

# DBTITLE 1,Write
target_tbl = f"{catalog_name}.{gold_schema_name}.delivery_performance"
(
    delivery_performance_ranked.write.mode("overwrite")
    .options(overwriteSchema="true")
    .saveAsTable(target_tbl)
)
