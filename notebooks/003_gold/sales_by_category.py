# Databricks notebook source
# DBTITLE 1,Imports
from olist_lakehouse.io import read_managed_table, write_managed_table
from pyspark.sql import Window
from pyspark.sql.functions import (
    avg,
    col,
    countDistinct,
    rank,
)
from pyspark.sql.functions import (
    count as _count,
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
# MAGIC ## Sales by category table
# MAGIC Which categories generate more revenue? Which one has the biggest average order ticket?
# MAGIC
# MAGIC **Table requirements:** 
# MAGIC
# MAGIC - Grouping by category (in english)
# MAGIC - Must be ordered by `revenue_rank`
# MAGIC - Only orders with `order_status` = `'delivered'`
# MAGIC
# MAGIC | Metric             | Description                                              |
# MAGIC | -------------------| ---------------------------------------------------------|
# MAGIC | `total_revenue`    | Sum of `price` of all sold items                         |
# MAGIC | `total_orders`     | Distinct orders containing the item from the category    |
# MAGIC | `total_items_sold` | Total items sold                                         |
# MAGIC | `avg_order_ticket` | Average order revenue (`total_revenue` / `total_orders`) |
# MAGIC | `avg_item_price`   | Average item price                                       |
# MAGIC | `revenue_rank`     | Revenue rank category                                    |

# COMMAND ----------

# DBTITLE 1,Read
orders = read_managed_table(catalog, "silver", "orders")
order_items = read_managed_table(catalog, "silver", "order_items")
products = read_managed_table(catalog, "silver", "products")

# COMMAND ----------

# DBTITLE 1,Joins
orders_and_products = order_items.join(products, on="product_id", how="left").join(orders, on="order_id", how="left")
orders_and_products_filtered = orders_and_products.filter("order_status = 'delivered'")

# COMMAND ----------

# DBTITLE 1,Group by product category
sales_by_category = orders_and_products_filtered.groupBy("product_category_name_english").agg(
    _sum("price").alias("total_revenue"),
    countDistinct("order_id").alias("total_orders"),
    _count("*").alias("total_items_sold"),
    (_sum("price") / countDistinct("order_id")).alias("avg_order_ticket"),
    avg("price").alias("avg_item_price"),
)

# COMMAND ----------

# DBTITLE 1,Window ranking
wn_revenue = Window.orderBy(col("total_revenue").desc(), col("avg_order_ticket").desc())

sales_by_category_ranked = sales_by_category.withColumn("revenue_rank", rank().over(wn_revenue))

# COMMAND ----------

# DBTITLE 1,Write
write_managed_table(
    sales_by_category_ranked,
    mode="overwrite",
    table_full_path=f"{catalog}.gold.sales_by_category",
    options={"overwriteSchema": "true"}
)