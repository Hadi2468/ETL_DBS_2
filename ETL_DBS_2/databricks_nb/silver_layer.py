# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql import functions as F, Window
from delta.tables import DeltaTable
import datetime

# COMMAND ----------

spark.sql("SET spark.sql.adaptive.enabled=true")

# COMMAND ----------

# MAGIC %run ./helper_function/helper_functions

# COMMAND ----------

# MAGIC %md
# MAGIC Create silver databse
# MAGIC

# COMMAND ----------

SRC_DB       = "samples.tpch"        # where original TPC‑H tables live (Delta).tpch.customer
ANALYTICS_DB = "s3catalog.tpch_star_silver"     # target database for the SCD2 tables
spark.sql(f"CREATE DATABASE IF NOT EXISTS {ANALYTICS_DB}")

# COMMAND ----------

# MAGIC %md
# MAGIC STAGING: read source tables

# COMMAND ----------

orders   = spark.table(f"{SRC_DB}.orders")          # ORDERDATE etc.
lineitem = spark.table(f"{SRC_DB}.lineitem")        # QUANTITY, EXTENDEDPRICE, DISCOUNT
part     = spark.table(f"{SRC_DB}.part")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Demand classification
# MAGIC https://frepple.com/blog/demand-classification/

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

