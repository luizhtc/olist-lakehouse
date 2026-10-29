# Databricks notebook source
# DBTITLE 1,Imports
from olist_lakehouse.io import read_managed_table, write_managed_table
from pyspark.sql.functions import col

# COMMAND ----------

# DBTITLE 1,Define parameters
dbutils.widgets.text("catalog", "")

# COMMAND ----------

# DBTITLE 1,Get parameters
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Order Items table
# MAGIC Enriched order items with sellers information.
# MAGIC
# MAGIC **Table requirements:**
# MAGIC
# MAGIC - Fields must be correctly typed
# MAGIC
# MAGIC **Expected schema:**
# MAGIC
# MAGIC | Field                  | Type   | Origin (bronze)           |
# MAGIC | -----------------------| -------| --------------------------|
# MAGIC | order_id               | STRING | olist_order_items_dataset |
# MAGIC | order_item_id          | INT    | olist_order_items_dataset |
# MAGIC | product_id             | STRING | olist_order_items_dataset |
# MAGIC | price                  | DOUBLE | olist_order_items_dataset |
# MAGIC | freight_value          | DOUBLE | olist_order_items_dataset |
# MAGIC | seller_id              | STRING | olist_sellers_dataset     |
# MAGIC | seller_city            | STRING | olist_sellers_dataset     |
# MAGIC | seller_state           | STRING | olist_sellers_dataset     |
# MAGIC | seller_zip_code_prefix | STRING | olist_sellers_dataset     |

# COMMAND ----------

# DBTITLE 1,Read
order_items = read_managed_table(
    catalog=catalog,
    schema="bronze",
    table="olist_order_items_dataset",
)
sellers = read_managed_table(
    catalog=catalog,
    schema="bronze",
    table="olist_sellers_dataset",
)

# COMMAND ----------

# DBTITLE 1,Transform
order_items_join = order_items.join(sellers, on="seller_id", how="left")
order_items_transformed = order_items_join.select(
    "order_id",
    col("order_item_id").cast("int"),
    "product_id",
    col("price").cast("double"),
    col("freight_value").cast("double"),
    "seller_id",
    "seller_city",
    "seller_state",
    "seller_zip_code_prefix",
)

# COMMAND ----------

# DBTITLE 1,Write
write_managed_table(
    order_items_transformed,
    mode="overwrite",
    table_full_path=f"{catalog}.silver.order_items",
    options={"overwriteSchema": "true"}
)