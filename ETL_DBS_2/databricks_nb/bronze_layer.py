# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "1"
# ///
# MAGIC %md
# MAGIC # Bronze Layer

# COMMAND ----------

# Import libraries
from pyspark.sql.types import *
from pyspark.sql import functions as F, Window
from delta.tables import DeltaTable
import datetime

# COMMAND ----------

## Enable AQE (Adaptive Query Execution)
# spark.conf.set("spark.sql.adaptive.enabled", "true")

# COMMAND ----------

# Creating bronze schema
src_db = "samples.tpch"      # Original source database
my_catalog = "dbs-project2"  # bronze layer database for raw data ingestion
bronze_schema = "BRONZE_DB"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{my_catalog}`.`{bronze_schema}`")

# COMMAND ----------

# Available catalogs
spark.sql("SHOW CATALOGS").show()

# COMMAND ----------

# Available schemas in `dbs-project2`
# spark.sql(f"SHOW SCHEMAS IN `{my_catalog}`").show()
spark.sql("SHOW SCHEMAS IN `dbs-project2`").show()

# COMMAND ----------

# Landing raw data (3 tables) from source database into Bronze layer
tables = ["orders", "lineitem", "part"]

for table in tables:
    df = spark.table(f"{src_db}.{table}")
    df.write.mode("overwrite").saveAsTable(f"`{my_catalog}`.`{bronze_schema}`.{table}")

# COMMAND ----------

# Validate one of tables
display(spark.table(f"`{my_catalog}`.`{bronze_schema}`.orders").limit(10))
                    
