# Databricks notebook source
def create_db(db_name: str, location: str | None = None):
    if location:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name} LOCATION '{location}'")
    else:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ####   Merge query document
# MAGIC
# MAGIC https://docs.databricks.com/aws/en/sql/language-manual/delta-merge-into

# COMMAND ----------

# MAGIC %md
# MAGIC #### SCD Document
# MAGIC
# MAGIC https://en.wikipedia.org/wiki/Slowly_changing_dimension
# MAGIC

# COMMAND ----------


def scd2_merge(target_full_name: str,
               source_df,
               business_key_cols: list[str],
               compare_cols: list[str],
               ts_col: str = "_start_ts",
               end_ts_col: str = "_end_ts",
               current_flag_col: str = "is_current"):
    tgt = DeltaTable.forName(spark, target_full_name)
    src_alias, tgt_alias = "s", "t"
    join_cond = " AND ".join([f"{src_alias}.{c} = {tgt_alias}.{c}"
                              for c in business_key_cols])
    change_cond = " OR ".join([f"{src_alias}.{c} <> {tgt_alias}.{c}"
                               for c in compare_cols])

    (
      tgt.alias(tgt_alias)
         .merge(source_df.alias(src_alias), join_cond)
         # 1️. Close current if any data col changed
         .whenMatchedUpdate(
              condition=f"{tgt_alias}.{current_flag_col}=true AND ({change_cond})",
              set   = {end_ts_col: F.current_timestamp(),
                       current_flag_col: F.lit(False)})
         # 2. Insert brand‑new business key
         .whenNotMatchedInsert(values={ **{c: f"{src_alias}.{c}" for c in source_df.columns},
                                        ts_col: F.current_timestamp(),
                                        end_ts_col: F.lit(None).cast("timestamp"),
                                        current_flag_col: F.lit(True)})
         .execute()
    )


# COMMAND ----------

# MAGIC %md
# MAGIC