import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

##bookings silver
@dlt.table(
    name = "stg_bookings"
)
def stg_bookings():
    df = spark.readStream.format("delta")\
        .load("/Volumes/workspace/bronze/bronzevolume/bookings/data/")
    
    return df

@dlt.view(
    name = "trans_bookings"
)
def transform_bookings():
    df = spark.readStream.table("stg_bookings")
    df = df.withColumn("amount", col("amount").cast(DoubleType()))\
        .withColumn("modifiedDate", current_timestamp())\
        .withColumn("booking_date", to_date(col("booking_date")))\
        .drop("_rescued_data")
    return df

rules = {
    "rule1" : "booking_id IS NOT NULL",
    "rule2" : "passenger_id IS NOT NULL"
}

@dlt.table(
    name = "silver_bookings"
)
@dlt.expect_all_or_drop(rules)
def silver():
    df = spark.readStream.table("trans_bookings")
    return df

###########################################################

#Flights silver
@dlt.view (
    name = "trans_flights"
)

def trans_flights():
    df = spark.readStream.format("delta") \
        .load("/Volumes/workspace/bronze/bronzevolume/flights/data/")

    df = df.withColumn("flight_date", to_date(col("flight_date")))\
        .withColumn("modifiedDate", current_timestamp())\
        .drop("_rescued_data")

    return df

dlt.create_streaming_table("silver_flights")

dlt.create_auto_cdc_flow(
    target = "silver_flights",
    source = "trans_flights",
    keys = ["flight_id"],
    sequence_by = col("modifiedDate"),
    stored_as_scd_type = 1
)

###########################################################

#Passengers silver

@dlt.view(
    name = "trans_passengers"
)
def trans_passengers():
    df = spark.readStream.format("delta")\
        .load("/Volumes/workspace/bronze/bronzevolume/customers/data/")

    df = df.withColumn("modifiedDate", current_timestamp())\
        .drop("_rescued_data")
    return df

dlt.create_streaming_table("silver_passengers")

dlt.create_auto_cdc_flow(
    target = "silver_passengers",
    source = "trans_passengers",
    keys = ["passenger_id"],
    sequence_by = col("modifiedDate"),
    stored_as_scd_type = 1
)

###########################################################

#Airports silver

@dlt.view(
    name = "trans_airports"
)
def trans_airports():
    df = spark.readStream.format("delta")\
        .load("/Volumes/workspace/bronze/bronzevolume/airports/data/")
    
    df = df.withColumn("modifiedDate", current_timestamp())\
        .drop("_rescued_data")

    return df

dlt.create_streaming_table("silver_airports")

dlt.create_auto_cdc_flow(
    target = "silver_airports",
    source = "trans_airports",
    keys = ["airport_id"],
    sequence_by = col("modifiedDate"),
    stored_as_scd_type = 1
)

##############################

# Silver Business

@dlt.table(
    name = "silver_business"
)
def silver_business():
    df = dlt.readStream("silver_bookings")\
        .join(dlt.read("silver_airports"), ["airport_id"])\
        .join(dlt.read("silver_passengers"), ["passenger_id"])\
        .join(dlt.read("silver_flights"), ["flight_id"]) \
        .drop("modifiedDate")
    return df  

# Materialized view
@dlt.table(
    name = "silver_business_mat"
)
def silver_business_mat():
    df = dlt.read("silver_business")
    return df
    

