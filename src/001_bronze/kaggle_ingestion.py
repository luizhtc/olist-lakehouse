# Databricks notebook source
# DBTITLE 1,TUTORIAL: Required secrets
# This notebook requires a Kaggle API Token, defined in 'kaggle' databricks secrets scope.

# Run the following Databricks CLI commands in a safe environment to create the scope and secret:
# databricks secrets create-scope kaggle
# databricks secrets put-secret kaggle KAGGLE_API_TOKEN --string-value "<your_token>"

# You can also create the scope + secret as python code:
# from databricks.sdk import WorkspaceClient
# w = WorkspaceClient()
# w.secrets.create_scope("kaggle")
# w.secrets.put_secret(
#     scope="kaggle",
#     key="KAGGLE_API_TOKEN",
#     string_value="<your_token>",
# )

# COMMAND ----------

# DBTITLE 1,Imports
import os

import kagglehub
from kagglehub import KaggleDatasetAdapter
from pyspark.sql.functions import current_timestamp

from olist_lakehouse.dataframe_transformations import all_columns_string

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Basic environment configuration

# COMMAND ----------

# MAGIC %md
# MAGIC ### 0.1. Environment variables

# COMMAND ----------

try:
    os.environ["KAGGLE_API_TOKEN"] = dbutils.secrets.get("kaggle", "KAGGLE_API_TOKEN")
except Exception as e:
    raise ValueError(
        "Could not load Kaggle API Token, be sure to follow the steps at the "
        "beginning of this notebook to setup the secrets!"
    ) from e

# COMMAND ----------

# MAGIC %md
# MAGIC ### 0.2. Ingestion parameters

# COMMAND ----------

dbutils.widgets.text("catalog_name", "olist")
dbutils.widgets.text("source_path", "olist_customers_dataset.csv")
dbutils.widgets.text("bronze_schema_name", "dev_luizhtcaldo_bronze")

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name")
source_path = dbutils.widgets.get("source_path")
bronze_schema_name = dbutils.widgets.get("bronze_schema_name")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. CSV ingestion into bronze schema

# COMMAND ----------

#DBTITLE 1,Downloading the source file from Kaggle and transforming it to a PySpark DF
pandas_df = kagglehub.dataset_load(
    adapter=KaggleDatasetAdapter.PANDAS,
    handle="olistbr/brazilian-ecommerce",
    path=source_path,
)

spark_df = (
    spark.createDataFrame(pandas_df)
    .transform(all_columns_string)
    .withColumn("last_updated_timestamp", current_timestamp())
)

# COMMAND ----------

#DBTITLE 1,Write to bronze table
# The olist dataset is small and static, so in this case we will re-download
# each full dataset and overwrite the table in every write
target_tbl = f"{catalog_name}.{bronze_schema_name}.{source_path.replace(".csv", "")}"
spark_df.write.mode("overwrite").options(overwriteSchema="true").saveAsTable(target_tbl)
