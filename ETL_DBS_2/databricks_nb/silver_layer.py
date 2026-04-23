# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "1"
# ///
# MAGIC %md
# MAGIC # Silver Layer

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

# Creating silver schema
my_catalog = "dbs-project2"
bronze_schema = "BRONZE_DB"
silver_schema = "SILVER_DB"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{my_catalog}`.`{silver_schema}`")

# COMMAND ----------

# Staging raw data as a Dataframe
df_orders = spark.table(f"`{my_catalog}`.`{bronze_schema}`.orders")
df_lineitem = spark.table(f"`{my_catalog}`.`{bronze_schema}`.lineitem")
df_part = spark.table(f"`{my_catalog}`.`{bronze_schema}`.part")
display(df_part.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Demand classification

# COMMAND ----------

min_day, max_day = orders.select(F.min("o_orderdate"), F.max("o_orderdate")).first()
min_day

# COMMAND ----------

# max_day = datetime.date(1992, 5, 2)

# COMMAND ----------

min_day, max_day = orders.select(F.min("o_orderdate"), F.max("o_orderdate")).first()
calendar = spark.range(0, (max_day - min_day).days + 1) \
                .select(F.expr(f"date_add('{min_day}', CAST(id AS INT))").alias("cal_date"))
calendar.display()

# COMMAND ----------

# MAGIC %md
# MAGIC PRODUCT METRICS  – ADI + demand class

# COMMAND ----------

daily_part_qty = (lineitem
                  .groupBy("l_partkey", "l_shipdate")
                  .agg(F.sum("l_quantity").alias("qty")))

daily_part_qty.display()

# COMMAND ----------

part.select("p_partkey").crossJoin(calendar)

# COMMAND ----------

part_daily = (part.select("p_partkey")
                   .crossJoin(calendar)
                   .join(daily_part_qty,
                         (daily_part_qty.l_partkey == part.p_partkey) &
                         (daily_part_qty.l_shipdate == calendar.cal_date),
                         "left")
                   .select(part.p_partkey.alias('partkey'),
                           calendar.cal_date.alias("date"),
                           F.coalesce("qty", F.lit(0)).alias("qty")))

part_daily.display()

# COMMAND ----------

stats_p = (part_daily.groupBy("partkey")
           .agg(F.count("*").alias("total_days"),
                F.sum(F.when(F.col("qty") > 0, 1).otherwise(0)).alias("nonzero_days"),
                F.expr("var_samp(qty) / pow(avg(qty),2)").alias("cv2"),
                F.avg("qty").alias("avg_qty")))
# stats_p.display()

# COMMAND ----------

product_metrics = (stats_p
                   .withColumn("ADI", F.col("total_days")/F.col("nonzero_days"))
                   .withColumn(
                        "demand_class",
                        F.when((F.col("ADI")<51) & (F.col("cv2")<=50), "Smooth")
                         .when((F.col("ADI")>=1.32) & (F.col("cv2")<0.49), "Intermittent")
                         .when((F.col("ADI")<1.32) & (F.col("cv2")>=0.49), "Erratic")
                         .otherwise("Lumpy"))
                   .select("partkey", "ADI", "cv2", "avg_qty", "demand_class"))

# COMMAND ----------

# product_metrics.display()

# COMMAND ----------

TARGET_PRODUCT = f"{ANALYTICS_DB}.product_demand_scd"

# COMMAND ----------

if not spark.catalog.tableExists(TARGET_PRODUCT):
    (product_metrics
        .withColumn("_start_ts", F.current_timestamp())
        .withColumn("_end_ts", F.lit(None).cast("timestamp"))
        .withColumn("is_current", F.lit(True))
        .write.format("delta")
        .option('overwriteSchema', 'true')
        .mode('overwrite')
        .option("delta.enableChangeDataFeed", "true")
        .option('path', f's3://testdatabricks1992/{TARGET_PRODUCT}')
        .saveAsTable(TARGET_PRODUCT))
else:
    scd2_merge(
        TARGET_PRODUCT,
        product_metrics,
        business_key_cols = ["partkey"],
        compare_cols      = ["ADI", "cv2", "avg_qty", "demand_class"]
    )

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,6g fxfgywtyvvo
# MAGIC
# MAGIC %sql
# MAGIC select * from s3catalog.tpch_star_silver.product_demand_scd limit 100;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct demand_class from s3catalog.tpch_star_silver.product_demand_scd;

# COMMAND ----------


