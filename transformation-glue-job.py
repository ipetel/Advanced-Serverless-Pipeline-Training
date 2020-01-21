import sys

#AWS Glue imports
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

# Spark imports
from pyspark.context import SparkContext
from pyspark.sql.functions import *
from pyspark.sql import SparkSession

glueContext = GlueContext(SparkContext.getOrCreate())
spark = SparkSession.builder.getOrCreate()

### Load dataset tables as DynamicFrames
trips_glueDf = glueContext.create_dynamic_frame.from_catalog(database="parquet", table_name="hamustaTaxi_tripdata")
locations_glueDf = glueContext.create_dynamic_frame.from_catalog(database="parquet", table_name="location_lookup_table")
days_glueDf = glueContext.create_dynamic_frame.from_catalog(database="parquet", table_name="dayofweek_lookup_table")

### Convert Glue DynamicFrames to Spark DataFrame
trips_sparkDf = trips_glueDf.toDF()
locations_sparkDf = locations_glueDf.toDF()
days_sparkDf = days_glueDf.toDF()

### Register the Spark DataFrames as a SQL temporary view
trips_sparkDf.createOrReplaceTempView("trips")
locations_sparkDf.createOrReplaceTempView("locations")
days_sparkDf.createOrReplaceTempView("days")

### Run transformation and create destination table
query='''
        SELECT dow.day_of_week,count_rides.ride_count AS ride_count
        FROM (
                  SELECT dayofweek(to_date(tpep_pickup_datetime)) AS day_of_week_id,count(*) AS ride_count
                  FROM trips AS trip
                  LEFT JOIN locations AS loc ON trip.pulocationid=loc.locationid
                  WHERE loc.borough='Queens'
                  GROUP BY dayofweek(to_date(tpep_pickup_datetime))
             ) AS count_rides
        LEFT JOIN days AS dow ON dow.day_of_week_id=count_rides.day_of_week_id
        '''

result_sparkSQL_df = spark.sql(query)

### Convert Spark DataFrame to Glue DynamicFrame
result_glue_df = DynamicFrame.fromDF(result_sparkSQL_df, glueContext, "result_glue_df")

### Write to data to S3
glueContext.write_dynamic_frame.from_options(
    frame = result_glue_df,
    connection_type = "s3",
    connection_options = {"path": "s3://allcloud-idan.petel-temp/training_advanced_pipeline/transformed/rideCount_Queens_per_dayOfWeek/"},
    format = "parquet")
    
### publish message to SNS topic
import boto3
import json

client = boto3.client('sns', region_name='eu-west-1')

response = client.publish(
    TopicArn='arn:aws:sns:eu-west-1:386027091365:ETL-cuba-burgol-c2',
    Message="ETL-cuba-burgol-c2 was completed successfully",
    Subject='ETL-cuba-burgol-c2 - AWS Glue',
    MessageStructure='string',
)
