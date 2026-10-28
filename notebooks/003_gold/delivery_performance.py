# Databricks notebook source
# DBTITLE 1,Imports
from olist_lakehouse.io import read_managed_table, write_managed_table
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
dbutils.widgets.text("catalog", "")

# COMMAND ----------

# DBTITLE 1,Get parameters
catalog = dbutils.widgets.get("catalog")

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
orders = read_managed_table(catalog, "silver", "orders")
order_items = read_managed_table(catalog, "silver", "order_items")

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
delivery_performance_ranked = delivery_performance.withColumn("performance_rank", rank().over(wn_performance))

# COMMAND ----------

# DBTITLE 1,Write
write_managed_table(
    delivery_performance_ranked,
    mode="overwrite",
    table_full_path=f"{catalog}.gold.delivery_performance",
    options={"overwriteSchema": "true"}
)