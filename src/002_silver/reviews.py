# Databricks notebook source
# DBTITLE 1,Imports
from pyspark.sql.functions import col, timestamp_diff, trim, when

from olist_lakehouse.dataframe_transformations import parse_timestamp

# COMMAND ----------

# DBTITLE 1,Define parameters
dbutils.widgets.text("catalog_name", "")
dbutils.widgets.text("bronze_schema_name", "")
dbutils.widgets.text("silver_schema_name", "")

catalog_name = dbutils.widgets.get("catalog_name")
bronze_schema_name = dbutils.widgets.get("bronze_schema_name")
silver_schema_name = dbutils.widgets.get("silver_schema_name")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Reviews table
# MAGIC Enriched reviews table with sentiment analysis and response metrics.
# MAGIC
# MAGIC **Table requirements:**
# MAGIC
# MAGIC - Create a `sentiment_bucket`:
# MAGIC   - `'negative'` → scores 1 and 2
# MAGIC   - `'neutral'` → score 3
# MAGIC   - `'positive'` → scores 4 and 5
# MAGIC - `has_comment` = `TRUE` when `review_comment_message` is not null and not empty
# MAGIC - `response_time_hours` = `review_creation_date` - `review_answer_timestamp`
# MAGIC
# MAGIC **Expected Schema:**
# MAGIC
# MAGIC | Field                   | Type      | Origin (bronze)             |
# MAGIC | ------------------------| ----------| ----------------------------|
# MAGIC | review_id               | STRING    | olist_order_reviews_dataset |
# MAGIC | order_id                | STRING    | olist_order_reviews_dataset |
# MAGIC | review_score            | INT       | olist_order_reviews_dataset |
# MAGIC | review_comment_title    | STRING    | olist_order_reviews_dataset |
# MAGIC | review_comment_message  | STRING    | olist_order_reviews_dataset |
# MAGIC | review_creation_date    | TIMESTAMP | olist_order_reviews_dataset |
# MAGIC | review_answer_timestamp | TIMESTAMP | olist_order_reviews_dataset |
# MAGIC | sentiment_bucket        | STRING    | logic                       |
# MAGIC | has_comment             | BOOLEAN   | logic                       |
# MAGIC | response_time_hours     | BIGINT    | logic                       |

# COMMAND ----------

# DBTITLE 1,Read
reviews = spark.read.table(
    f"{catalog_name}.{bronze_schema_name}.olist_order_reviews_dataset"
)

# COMMAND ----------

# DBTITLE 1,Transform
reviews_select = reviews.select(
    "review_id",
    "order_id",
    col("review_score").cast("int"),
    "review_comment_title",
    "review_comment_message",
    parse_timestamp(col("review_creation_date")).alias("review_creation_date"),
    parse_timestamp(col("review_answer_timestamp")).alias("review_answer_timestamp"),
)

reviews_transformed = (
    reviews_select.withColumn(
        "sentiment_bucket",
        when(col("review_score").isin([1, 2]), "negative")
        .when(col("review_score") == 3, "neutral")
        .when(col("review_score").isin([4, 5]), "positive")
        .otherwise(None),
    )
    .withColumn(
        "has_comment",
        when(trim(col("review_comment_message")).isNull(), False).otherwise(True),
    )
    .withColumn(
        "response_time_hours",
        timestamp_diff(
            "HOUR", col("review_creation_date"), col("review_answer_timestamp")
        ),
    )
)

# COMMAND ----------

# DBTITLE 1,Write
target_tbl = f"{catalog_name}.{silver_schema_name}.reviews"
(
    reviews_transformed.write.mode("overwrite")
    .options(overwriteSchema="true")
    .saveAsTable(target_tbl)
)
