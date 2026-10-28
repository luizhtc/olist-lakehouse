# Databricks notebook source
# DBTITLE 1,Imports
from olist_lakehouse.io import read_managed_table, write_managed_table
from olist_lakehouse.transformations import parse_timestamp
from pyspark.sql.functions import (
    col,
    collect_set,
    datediff,
    when,
)
from pyspark.sql.functions import (
    max as _max,
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
# MAGIC ## Orders table
# MAGIC Contains a complete order information view bringing together orders, customers and payments.
# MAGIC
# MAGIC **Table requirements:**
# MAGIC
# MAGIC - Fields must be correctly typed with `TIMESTAMP`
# MAGIC - Aggregate payments by order
# MAGIC   - `total_payment_value` - sum of paid value
# MAGIC   - `max_installments` - max numbers of installments
# MAGIC   - `payments_types_used` - a list of distinct payment types used
# MAGIC - Field `delivery_delay_days`
# MAGIC   - difference of days between `order_delivered_customer_date` and `order_estimated_delivery_date`
# MAGIC   - orders not delivered must return `NULL`
# MAGIC - Field `is_late`
# MAGIC   - `TRUE` when `delivery_delay_days` > 0 
# MAGIC
# MAGIC **Expected schema:**
# MAGIC
# MAGIC | Field                    | Type      | Origin (bronze)                    |
# MAGIC | -------------------------| ----------| -----------------------------------|
# MAGIC | order_id                 | STRING    | olist_orders_dataset               |
# MAGIC | customer_unique_id       | STRING    | olist_customers_dataset            |
# MAGIC | customer_state           | STRING    | olist_customers_dataset            |
# MAGIC | order_status             | STRING    | olist_orders_dataset               |
# MAGIC | order_purchase_timestamp | TIMESTAMP | olist_orders_dataset               |
# MAGIC | total_payment_value      | DOUBLE    | olist_order_payments_dataset (agg) |
# MAGIC | max_installments         | INT       | olist_order_payments_dataset (agg) |
# MAGIC | payment_types_used       | STRING    | olist_order_payments_dataset (agg) |
# MAGIC | delivery_delay_days      | INT       | logic                              |
# MAGIC | is_late                  | BOOLEAN   | logic                              |

# COMMAND ----------

# DBTITLE 1,Read sources
orders = read_managed_table(
    catalog=catalog,
    schema="bronze",
    table="olist_orders_dataset",
)
customers = read_managed_table(
    catalog=catalog,
    schema="bronze",
    table="olist_customers_dataset",
)
payments = read_managed_table(
    catalog=catalog,
    schema="bronze",
    table="olist_order_payments_dataset",
)

# COMMAND ----------

# DBTITLE 1,Aggregate payments table
payments = (
    payments
    .withColumn("payment_value", col("payment_value").cast("double"))
    .withColumn("payment_installments", col("payment_installments").cast("int"))
)

payments_agg = (
    payments
    .groupBy("order_id")
    .agg(
        _sum(col("payment_value")).alias("total_payment_value"),
        _max(col("payment_installments")).alias("max_installments"),
        collect_set(col("payment_type")).alias("payment_types_used"),
    )
    .withColumn("payment_types_used", col("payment_types_used").cast("string"))
)

# COMMAND ----------

# DBTITLE 1,Adjust to schema requirements
orders_join = orders.join(customers, on="customer_id", how="left").join(payments_agg, on="order_id", how="left")
orders_transformed = orders_join.select(
    "order_id",
    "customer_unique_id",
    "customer_state",
    "order_status",
    parse_timestamp("order_purchase_timestamp").alias("order_purchase_timestamp"),
    "total_payment_value",
    "max_installments",
    "payment_types_used",
    when(col("order_delivered_customer_date").isNull(), None)
    .otherwise(
        datediff(
            parse_timestamp("order_delivered_customer_date"),
            parse_timestamp("order_estimated_delivery_date"),
        )
    )
    .alias("delivery_delay_days"),
).withColumn("is_late", when(col("delivery_delay_days") > 0, True).otherwise(False))

# COMMAND ----------

# DBTITLE 1,Write
write_managed_table(
    orders_transformed,
    mode="overwrite",
    table_full_path=f"{catalog}.silver.orders",
    options={"overwriteSchema": "true"}
)