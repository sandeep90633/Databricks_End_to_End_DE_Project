# Databricks notebook source
# MAGIC %md
# MAGIC #### Parameters

# COMMAND ----------

catalog = "workspace"

source_schema = "silver"

source_object = "silver_bookings"

cdc_col = "modifiedDate"

backdated_refresh = ""

fact_table = f"{catalog}.{source_schema}.{source_object}"

target_schema = "gold"

target_object = "FactBookings"

fact_key_columns = ["DimPassengersKey","DimFlightsKey","DimAirportsKey", "booking_date"]

# COMMAND ----------

dimensions = [
    {
        "table" : f"{catalog}.{target_schema}.DimPassengers",
        "alias" : "DimPassengers",
        "join_keys" : [("passenger_id", "passenger_id")]
    },
    {
        "table" : f"{catalog}.{target_schema}.DimFlights",
        "alias" : "DimFlights",
        "join_keys" : [("flight_id", "flight_id")]
    },
    {
        "table" : f"{catalog}.{target_schema}.DimAirports",
        "alias" : "DimAirports",
        "join_keys" : [("airport_id", "airport_id")]
    }
]

# columns to keep in the fact eve 
fact_columns = ["amount","booking_date","modifiedDate"]

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
# MAGIC #### DYNAMIC FACT QUERY

# COMMAND ----------

def generate_fact_query_incremental(fact_table, dimensions, fact_columns, cdc_col, processing_date):
    fact_alias = "f"

    select_cols = [f"{fact_alias}.{col}" for col in fact_columns]

    join_clauses = []
    for dim in dimensions:
        table_full = dim["table"]
        table_alias = dim["alias"]
        table_name = table_full.split(".")[-1]
        surrogate_key = f"{table_alias}.{table_name}Key"
        select_cols.append(surrogate_key)

        on_coditions = [
            f"{fact_alias}.{fk} = {table_alias}.{dk}" for fk, dk in dim['join_keys']
        ]

        join_clause = f"LEFT JOIN {table_full} {table_alias} ON " + " AND ".join(on_coditions)
        join_clauses.append(join_clause)
    
    select_clause = ",\n    ".join(select_cols)
    joins = "\n".join(join_clauses)

    where_clause = f"{fact_alias}.{cdc_col} >= DATE('{processing_date}')"
    
    query = f"""
SELECT
    {select_clause}
FROM
    {fact_table} {fact_alias}
{joins}
WHERE {where_clause}
""".strip()

    return query

# COMMAND ----------

query = generate_fact_query_incremental(fact_table, dimensions, fact_columns, cdc_col, last_load)

# COMMAND ----------

print(query)

# COMMAND ----------

df_fact = spark.sql(query)

# COMMAND ----------

# MAGIC %md
# MAGIC #### UPSERT FACT TABLE

# COMMAND ----------

# MAGIC %md
# MAGIC Fact Key Merge Condidtion

# COMMAND ----------

fact_key_cols_str = " AND ".join([f"src.{col} = tgt.{col}" for col in fact_key_columns])

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if spark.catalog.tableExists(f"{catalog}.{target_schema}.{target_object}"):

    dlt_object = DeltaTable.forName(spark, f"{catalog}.{target_schema}.{target_object}")

    dlt_object.alias("tgt").merge(df_fact.alias("src"), fact_key_cols_str)\
                            .whenMatchedUpdateAll(condition = f"src.{cdc_col} >= tgt.{cdc_col}")\
                            .whenNotMatchedInsertAll()\
                            .execute()
else:
    df_fact.write.format('delta')\
        .mode("append")\
        .saveAsTable(f"{catalog}.{target_schema}.{target_object}")

# COMMAND ----------

