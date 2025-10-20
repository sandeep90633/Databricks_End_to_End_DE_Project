# Databricks notebook source
import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

@dlt.table(
    name = "stg_bookings"
)
def stg_bookings():
    df = spark.readStream.format("delta")\
        .load("/Volumes/workspace/bronze/bronzevolume/bookings/data/")
    
    return df

# COMMAND ----------

@dlt.view(
    name = "transformed_bookings"
)
def transform_bookings():
    df = spark.readStream.table("stg_bookings")
    df = df.withColumn("amount", col("amount").cast(DoubleType()))\
        .withColumn("modifiedDate", current_timestamp())\
        .withColumn("booking_date", to_date(col("booking_date")))\
        .drop("_rescued_data")
    return df

# COMMAND ----------

rules = {
    "rule1" : "booking_id IS NOT NULL",
    "rule2" : "passenger_id IS NOT NULL"
}

# COMMAND ----------

@dlt.table(
    name = "silver"
)
@dlt.expect_all_or_drop(rules)
def silver():
    df = spark.readStream.table("transformed_bookings")
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC Flights data

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df = spark.read.format("delta")\
    .load("/Volumes/workspace/bronze/bronzevolume/flights/data/")

display(df)


# COMMAND ----------

df = df.withColumn("flight_date", to_date(col("flight_date")))\
    .withColumn("modifiedDate", current_timestamp())\
    .drop("_rescued_data")
    
display(df)


# COMMAND ----------

# MAGIC %md
# MAGIC Customers Data

# COMMAND ----------

df = spark.read.format("delta")\
        .load("/Volumes/workspace/bronze/bronzevolume/customers/data/")

display(df)

# COMMAND ----------

df = df.withColumn("modifiedDate", current_timestamp())\
    .drop("_rescued_data")

# COMMAND ----------

# MAGIC %md
# MAGIC Airports Data

# COMMAND ----------

df = df.withColumn("modifiedDate", current_timestamp())\
    .drop("_rescued_data")

# COMMAND ----------

# MAGIC %md
# MAGIC Testing the Quality of data after silver_dlt_pipeline run

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.silver.silver_airports

# COMMAND ----------

