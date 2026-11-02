# Databricks notebook source
from pyspark.sql import Window
from pyspark.sql.functions import (
    col,
    concat_ws,
    countDistinct,
    date_diff,
    lit,
    ntile,
)
from pyspark.sql.functions import (
    max as _max,
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

# Since the dataset is fronze, we simulate the current date via this constant.
# Format must be YYYY-MM-dd
CURRENT_DATE = "2019-01-01"

# COMMAND ----------

orders = spark.read.table(f"{catalog_name}.{silver_schema_name}.orders")

# COMMAND ----------

rfm_dimensions = orders.groupby("customer_unique_id").agg(
    date_diff(lit(CURRENT_DATE), _max(col("order_purchase_timestamp"))).alias(
        "recency"
    ),
    countDistinct(col("order_id")).alias("frequency"),
    _sum(col("total_payment_value")).alias("monetary"),
)

# COMMAND ----------

wn_recency = Window.orderBy("recency")
wn_frequency = Window.orderBy("frequency")
wn_monetary = Window.orderBy("monetary")

rfm = (
    rfm_dimensions.withColumn("q_recency", ntile(4).over(wn_recency))
    .withColumn("q_frequency", ntile(4).over(wn_frequency))
    .withColumn("q_monetary", ntile(4).over(wn_monetary))
    .withColumn(
        "rfm_segment",
        concat_ws("-", col("q_recency"), col("q_frequency"), col("q_monetary")),
    )
    .drop("q_recency", "q_frequency", "q_monetary")
)

# COMMAND ----------

target_tbl = f"{catalog_name}.{gold_schema_name}.rfm_segment"
rfm.write.mode("overwrite").options(overwriteSchema="true").saveAsTable(target_tbl)
