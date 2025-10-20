# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE VOLUME workspace.raw.rawvolume

# COMMAND ----------

dbutils.fs.mkdirs("/Volumes/workspace/raw/rawvolume/rawdata/flights")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA workspace.gold

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE  VOLUME workspace.silver.silvervolume

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`/Volumes/workspace/bronze/bronzevolume/airports/data`

# COMMAND ----------

