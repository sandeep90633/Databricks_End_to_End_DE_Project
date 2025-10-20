# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating Variables

# COMMAND ----------

# MAGIC %md
# MAGIC flight dimensions variables

# COMMAND ----------

# key_cols_list = eval("['flight_id']")

# cdc_col = "modifiedDate"

# backdated_refresh = ""

# catalog = "workspace"

# source_object = "silver_flights"

# source_schema = "silver"

# target_schema = "gold"

# target_object = "DimFlights"

# surragate_key = "DimFlightsKey"

# COMMAND ----------

# MAGIC %md
# MAGIC Airport Dimension variables

# COMMAND ----------

# key_cols_list = eval("['airport_id']")

# cdc_col = "modifiedDate"

# backdated_refresh = ""

# catalog = "workspace"

# source_object = "silver_airports"

# source_schema = "silver"

# target_schema = "gold"

# target_object = "DimAirports"

# surragate_key = "DimAirportsKey"

# COMMAND ----------

# MAGIC %md
# MAGIC Customers Dimension Variables

# COMMAND ----------

key_cols_list = eval("['passenger_id']")

cdc_col = "modifiedDate"

backdated_refresh = ""

catalog = "workspace"

source_object = "silver_passengers"

source_schema = "silver"

target_schema = "gold"

target_object = "Dimpassengers"

surragate_key = "DimpassengersKey"

# COMMAND ----------

# MAGIC %md
# MAGIC ### INCREMENTAL DATA INGESTION

# COMMAND ----------

# MAGIC %md
# MAGIC #### Last Load Date

# COMMAND ----------

if len(backdated_refresh) == 0:

  if spark.catalog.tableExists(f"{catalog}.{target_schema}.{target_object}"):
    last_load = spark.sql(f"SELECT max({cdc_col}) FROM {catalog}.{target_schema}.{target_object}").collect()[0][0]
  else:
    last_load = "1900-01-01 00:00:00"
  
else:
    last_load = backdated_refresh


last_load

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Source new data

# COMMAND ----------

df_src = spark.sql(f"SELECT * FROM {source_schema}.{source_object} WHERE {cdc_col} > '{last_load}'")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Target data

# COMMAND ----------

if spark.catalog.tableExists(f"{catalog}.{target_schema}.{target_object}"):
    key_cols_str_incremental =", ".join(key_cols_list)

    df_tgt = spark.sql(f"SELECT {key_cols_str_incremental}, {surragate_key}, create_date, update_date FROM {catalog}.{target_schema}.{target_object}")
else:
    key_cols_str_initial = [f"'' AS {i}"for i in key_cols_list]
    key_cols_str_initial = ", ".join(key_cols_str_initial)
    df_tgt = spark.sql(f"SELECT {key_cols_str_initial}, CAST(0 as INT) AS {surragate_key}, CAST('1900-01-01 00:00:00' as timestamp) AS create_date, CAST('1900-01-01 00:00:00' as timestamp) AS update_date WHERE 1=2")

# COMMAND ----------

# MAGIC %md
# MAGIC Condition for Joining source data and target data

# COMMAND ----------

join_condition = " AND ".join([f"src.{i} = tgt.{i}" for i in key_cols_list])


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Join tables

# COMMAND ----------

df_src.createOrReplaceTempView("src")
df_tgt.createOrReplaceTempView("tgt")

df_join = spark.sql(f"""
          SELECT
            src.*,
            tgt.{surragate_key} AS {surragate_key},
            tgt.create_date,
            tgt.update_date
          FROM
            src
          LEFT JOIN
            tgt
          ON {join_condition}
          """)

# COMMAND ----------

df_old = df_join.filter(col(f'{surragate_key}').isNotNull())
df_new = df_join.filter(col(f'{surragate_key}').isNull())

# COMMAND ----------

# MAGIC %md
# MAGIC #### Enriching df_old

# COMMAND ----------

df_old_enr = df_old.withColumn('update_date', current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC #### Enriching df_new

# COMMAND ----------

if spark.catalog.tableExists(f"{catalog}.{target_schema}.{target_object}"):
    max_surragate_key = spark.sql(f"SELECT MAX({surragate_key}) FROM {catalog}.{target_schema}.{target_object}").collect()[0][0]
    df_new_enr = df_new.withColumn(f'{surragate_key}', lit(max_surragate_key) + lit(1) + monotonically_increasing_id())\
                .withColumn('create_date', current_timestamp())\
                .withColumn('update_date', current_timestamp())
else:
    max_surragate_key = 0
    df_new_enr = df_new.withColumn(f'{surragate_key}', lit(max_surragate_key) + lit(1) + monotonically_increasing_id())\
                .withColumn('create_date', current_timestamp())\
                .withColumn('update_date', current_timestamp())
    

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Unioning old and new records

# COMMAND ----------

df_union = df_old_enr.union(df_new_enr)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### UPSERT

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if spark.catalog.tableExists(f"{catalog}.{target_schema}.{target_object}"):
    dlt_object = DeltaTable.forName(spark, f"{catalog}.{target_schema}.{target_object}")
    dlt_object.alias("tgt").merge(df_union.alias("src"), f"tgt.{surragate_key} = src.{surragate_key}")\
                            .whenMatchedUpdateAll(condition = f"src.{cdc_col} >= tgt.{cdc_col}")\
                            .whenNotMatchedInsertAll()\
                            .execute()
else:
    df_union.write.format('delta')\
        .mode("append")\
        .saveAsTable(f"{catalog}.{target_schema}.{target_object}")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.gold.dimpassengers WHERE passenger_id = 'P0049'

# COMMAND ----------

