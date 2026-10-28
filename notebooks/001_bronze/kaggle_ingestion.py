# Databricks notebook source
# DBTITLE 1,Required secrets tutorial
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

from pyspark.sql.functions import current_timestamp

from olist_lakehouse.data_sources import load_into_pandas_dataframe
from olist_lakehouse.io import write_managed_table
from olist_lakehouse.transformations import all_columns_string

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Basic environment configuration

# COMMAND ----------

# MAGIC %md
# MAGIC ### 0.1. Environment variables

# COMMAND ----------

# DBTITLE 1,Defining KAGGLE_API_TOKEN env variable
try:
    os.environ["KAGGLE_API_TOKEN"] = dbutils.secrets.get("kaggle", "KAGGLE_API_TOKEN")
except Exception as e:
    raise ValueError(
        "Could not load Kaggle API Token, be sure to follow the steps at the "
        "beginning of this notebook to set the secrets!"
    ) from e

# COMMAND ----------

# MAGIC %md
# MAGIC ### 0.2. Ingestion parameters

# COMMAND ----------

# DBTITLE 1,Define parameters
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("dataset", "")

# COMMAND ----------

# DBTITLE 1,Get parameters
catalog = dbutils.widgets.get("catalog")
dataset = dbutils.widgets.get("dataset")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. CSV ingestion into bronze schema

# COMMAND ----------

# DBTITLE 1,CSV read and table write
table_output_name = f"{catalog}.bronze.{dataset.replace(".csv", "")}"
pandas_df = load_into_pandas_dataframe(
    handle="olistbr/brazilian-ecommerce",
    path=dataset,
)

spark_df = (
    spark.createDataFrame(pandas_df)
    .transform(all_columns_string)
    .withColumn("last_updated_timestamp", current_timestamp())
)

# The olist dataset is small and static, so in this case we will re-download
# each full dataset and overwrite the table in every write
write_managed_table(
    spark_df,
    mode="overwrite",
    table_full_path=table_output_name,
    options={"overwriteSchema": "true"}
)
