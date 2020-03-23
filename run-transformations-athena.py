import boto3
import json

### init params
client = boto3.client('athena')
s3_bucket_name='<BUCKET-NAME>'
dest_database_name='<BUCKET-FOLDER-PATH>' 
dest_s3_output='s3://{}/{}'.format(s3_bucket_name,dest_database_name)

### quieres

#Get the count of rides that picked up passangers from 'Queens' per day of week name
query= '''
    CREATE TABLE "transformed"."rideCount_Queens_per_dayOfWeek"
    WITH (format='PARQUET') AS
    SELECT "dow"."day_of_week","count_rides"."ride count" AS "ride_count"
    FROM (
          SELECT day_of_week(date_parse("tpep_pickup_datetime",'%Y-%m-%d %H:%i:%s')) AS "day_of_week_id",count(*) AS "ride count"
          FROM "parquet"."hamustaTaxi_tripdata" AS "trip"
          LEFT JOIN "parquet"."location_lookup_table" AS "loc" ON "trip"."pulocationid"="loc"."locationid"
          WHERE "loc"."borough"='Queens'
          GROUP BY day_of_week(date_parse("tpep_pickup_datetime",'%Y-%m-%d %H:%i:%s'))
         ) AS "count_rides"
    LEFT JOIN "parquet"."dayofweek_lookup_table" AS "dow" ON "dow"."day_of_week_id"="count_rides"."day_of_week_id"
    '''

### functions

def start_query_execution(query):
    return client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': dest_database_name
        },
        ResultConfiguration={
            'OutputLocation': dest_s3_output,
        }
    )

def lambda_handler(event, context):
    res={}
    res=start_query_execution(query)
    
    return {
        'status':'SUCCEEDED',
        'output':json.dumps(res)
    }
