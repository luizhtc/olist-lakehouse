# Databricks notebook source
# DBTITLE 1,Imports
from olist_lakehouse.io import read_managed_table, write_managed_table
from olist_lakehouse.transformations import calculate_dimensional_metrics
from pyspark.sql.functions import coalesce, col, lit

# COMMAND ----------

# DBTITLE 1,Define parameters
dbutils.widgets.text("catalog", "")

# COMMAND ----------

# DBTITLE 1,Get parameters
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Products table
# MAGIC Enriched products table with product name translation and dimensional metrics.
# MAGIC
# MAGIC **Table requirements:**
# MAGIC
# MAGIC - Translate `product_category_name` via join with `product_category_name_translation`
# MAGIC - Products without category must replace `NULL` for `'unknown'`
# MAGIC - Rename incorrect field names (`lenght` → `length`)
# MAGIC - Calculate product dimensional metrics
# MAGIC   - `product_volume_cm3` = `product_length_cm` x `product_height_cm` x `product_width_cm`
# MAGIC   - `density_g_cm3` = `product_weight_g` / `product_volume_cm3`
# MAGIC
# MAGIC **Expected Schema:**
# MAGIC
# MAGIC | Field                         | Type   | Origin (bronze)                   |
# MAGIC | ------------------------------| -------| ----------------------------------|
# MAGIC | product_id                    | STRING | olist_products_dataset            |
# MAGIC | product_category_name         | STRING | olist_products_dataset            |
# MAGIC | product_category_name_english | STRING | product_category_name_translation |
# MAGIC | product_name_length           | INT    | olist_products_dataset            |
# MAGIC | product_description_length    | INT    | olist_products_dataset            |
# MAGIC | product_photos_qty            | INT    | olist_products_dataset            |
# MAGIC | product_weight_g              | INT    | olist_products_dataset            |
# MAGIC | product_volume_cm3            | INT    | olist_products_dataset            |
# MAGIC | density_g_cm3                 | DOUBLE | logic                             |

# COMMAND ----------

# DBTITLE 1,Read sources
products = read_managed_table(
    catalog=catalog,
    schema="bronze",
    table="olist_products_dataset",
)
translations = read_managed_table(
    catalog=catalog,
    schema="bronze",
    table="product_category_name_translation",
)

# COMMAND ----------

# DBTITLE 1,Transform
products_join = products.join(translations, on="product_category_name", how="left")

# Transforming the necessary columns for calculations to int
products_join = products_join.withColumns({
    column: col(column).cast("float").cast("int") for column in [
        "product_length_cm",
        "product_height_cm",
        "product_width_cm",
        "product_weight_g"
    ]
})
products_calc = calculate_dimensional_metrics(products_join)

products_transformed = products_calc.select(
    "product_id",
    coalesce(col("product_category_name"), lit("desconhecido")).alias("product_category_name"),
    coalesce(col("product_category_name_english"), lit("unknown")).alias("product_category_name_english"),
    col("product_name_lenght").cast("float").cast("int").alias("product_name_length"),
    col("product_description_lenght").cast("float").cast("int").alias("product_description_length"),
    col("product_photos_qty").cast("float").cast("int").alias("product_photos_qty"),
    col("product_weight_g").cast("float").cast("int").alias("product_weight_g"),
    "product_volume_cm3",
    "density_g_cm3",
)

# COMMAND ----------

# DBTITLE 1,Write
write_managed_table(
    products_transformed,
    mode="overwrite",
    table_full_path=f"{catalog}.silver.products",
    options={"overwriteSchema": "true"}
)