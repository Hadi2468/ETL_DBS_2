# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql import functions as F, Window
from delta.tables import DeltaTable

# COMMAND ----------

spark.sql("SET spark.sql.adaptive.enabled=true")

# COMMAND ----------

SRC_DB       = "samples.tpch"        # where original TPC‑H tables live (Delta).tpch.customer
ANALYTICS_DB = "s3catalog.tpch_star_silver"     # target database for the SCD2 tables
spark.sql(f"CREATE DATABASE IF NOT EXISTS {ANALYTICS_DB}")

# COMMAND ----------

# MAGIC %run ./helper_function/helper_functions

# COMMAND ----------

orders   = spark.table(f"{SRC_DB}.orders")          # ORDERDATE etc.
lineitem = spark.table(f"{SRC_DB}.lineitem")        # QUANTITY, EXTENDEDPRICE, DISCOUNT
part     = spark.table(f"{SRC_DB}.part")
customer = spark.table(f"{SRC_DB}.customer")

# COMMAND ----------

w = Window.partitionBy("o_custkey").orderBy("o_orderdate")

api_df = (orders.select("o_custkey", "o_orderdate")
          .withColumn("prev_date", F.lag("o_orderdate").over(w))
          .withColumn("interval_days", F.datediff("o_orderdate", "prev_date"))
          .groupBy("o_custkey")
          .agg(F.avg("interval_days").alias("avg_days_between_orders"),
               F.min("o_orderdate").alias("first_order"),
               F.max("o_orderdate").alias("last_order"),
               F.count("*").alias("order_count")))


api_df = api_df.withColumn(
    "purchase_freq_per_year",
    F.round(F.when(
        F.datediff("last_order","first_order") > 0,
        F.round(
            F.col("order_count") / (F.datediff("last_order","first_order")/365.25),
            2
        )
    ).otherwise(0.0),2)
)
# api_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC  Net spend (extendedprice*(1-discount))

# COMMAND ----------

net_spend = (lineitem
             .withColumn("net_price",
                         F.col("l_extendedprice")*(1 - F.col("l_discount")))
             .groupBy("l_orderkey")
             .agg(F.sum("net_price").alias("order_net")))

cust_spend = (orders.join(net_spend,net_spend["l_orderkey"] == orders["o_orderkey"])
                    .join(customer,customer["c_custkey"] == orders["o_custkey"]))
                    

# COMMAND ----------

cust_spend = (cust_spend.groupBy("c_custkey")
                    .agg(F.sum("order_net").alias("lifetime_value")))

# COMMAND ----------

# cust_spend.display()

# COMMAND ----------

cust_metrics = (api_df.join(cust_spend,cust_spend["c_custkey"] == api_df["o_custkey"])
                .withColumn(
                    "interval_bucket",
                    F.when(F.col("avg_days_between_orders") <= 30,  "≤30 days")
                     .when(F.col("avg_days_between_orders") <= 90,  "31‑90 days")
                     .when(F.col("avg_days_between_orders") <= 180, "91‑180 days")
                     .otherwise(">180 days"))
                .select("c_custkey",
                        "avg_days_between_orders",
                        "purchase_freq_per_year",
                        "lifetime_value",
                        "interval_bucket"))

# COMMAND ----------

# cust_metrics.display()

# COMMAND ----------

TARGET_CUST = f"{ANALYTICS_DB}.customer_metrics_scd"

if not spark.catalog.tableExists(TARGET_CUST):
    (cust_metrics
        .withColumn("_start_ts", F.current_timestamp())
        .withColumn("_end_ts", F.lit(None).cast("timestamp"))
        .withColumn("is_current", F.lit(True))
        .write.format("delta")
        .option('overwriteSchema', 'true')
        .mode('overwrite')
        .option("delta.enableChangeDataFeed","true")
        .saveAsTable(TARGET_CUST))
else:
    scd2_merge(
        TARGET_CUST,
        cust_metrics,
        business_key_cols = ["c_custkey"],
        compare_cols      = ["avg_days_between_orders",
                             "purchase_freq_per_year",
                             "lifetime_value",
                             "interval_bucket"]
    )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from s3catalog.tpch_star_silver.customer_metrics_scd limit 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC select interval_bucket
# MAGIC , count(*) from s3catalog.tpch_star_silver.customer_metrics_scd group by interval_bucket
# MAGIC  order by count(*) desc limit 10;

# COMMAND ----------

