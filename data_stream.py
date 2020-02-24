import logging, time
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf

# Done:Create a schema for incoming resources
schema = StructType([
    StructField( "crime_id", StringType(), False),
    StructField( "original_crime_type_name", StringType(), True),
    StructField( "report_date", StringType(), True),
    StructField( "call_date", StringType(), True),
    StructField( "offense_date", StringType(), True),
    StructField( "call_time", StringType(), True),
    StructField( "call_date_time", StringType(), True),
    StructField( "disposition", StringType(), True),
    StructField( "address", StringType(), True),
    StructField( "city", StringType(), True),
    StructField( "state", StringType(), True),
    StructField( "agency_id", StringType(), True),
    StructField( "address_type", StringType(), True),
    StructField( "common_location", StringType(), True)
])


def run_spark_job(spark):

    # Done:Create Spark Configuration
    # Create Spark configurations with max offset of 200 per trigger
    # set up orrect bootstrap server and port
    df = spark \
        .readStream \
        .format("kafka") \
        .option("subscribe", "police-department-calls-for-service") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", 48000) \
        .option("stopGracefullyOnShutdown", "true") \
        .load()


    # Show schema for the incoming resources for checks
    df.printSchema()

    # Done: extract the correct column from the kafka input resources
    # Take only value and convert it to String
    kafka_df =df.selectExpr("CAST(value AS string)")

    service_table = kafka_df\
        .select(psf.from_json(psf.col('value'), schema).alias("DF"))\
        .select("DF.*")
    
    service_table.printSchema()
    
    # Done: select original_crime_type_name and disposition
    distinct_table = service_table.repartition(16).select('original_crime_type_name',
                                          'disposition', 
                                           psf.to_timestamp(
                                               psf.col("call_date_time"), 
                                               "yyyy-MM-dd'T'HH:mm:ss.SSS").alias("call_date_time"))

    
    # Done: count the number of original crime type
    agg_df = distinct_table.withWatermark("call_date_time", "10 minutes") \
        .groupBy(psf.window('call_date_time', '1 hours'),
                 'disposition','original_crime_type_name') \
        .count().orderBy('window', 'disposition')

   
    # Done: Q1. Submit a screen shot of a batch ingestion of the aggregation
    # Done: write output stream
    query = agg_df.writeStream \
       .format("console") \
       .outputMode("complete") \
       .trigger(once=True) \
       .option('truncate', False) \
       .start()
       
    # Done: attach a ProgressReporter
    query.awaitTermination()
    print("RECENT PROGRESS:\n", query.recentProgress, "\n")
    print("LAST PROGRESS:\n", query.lastProgress, "\n")
    print("STATUS:\n", query.status, "\n")
    
    print("Sleep 20s for taking screenshot")
    time.sleep(20)
    
    # Done: get the right radio code json path
    radio_code_json_filepath = "/home/workspace/radio_code.json"
    radio_code_df = spark.read.json(radio_code_json_filepath, multiLine=True )
    radio_code_df.printSchema()
   
    # clean up your data so that the column names match on radio_code_df and agg_df
    # we will want to join on the disposition code

    # Done: rename disposition_code column to disposition
    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")

    # Done: join on disposition column
    df_disposition = agg_df.repartition(16).join(radio_code_df, agg_df.disposition == radio_code_df.disposition, 
                                 'left').drop(agg_df.disposition)
    join_query = df_disposition.writeStream \
       .format("console") \
       .outputMode("complete") \
       .trigger(processingTime= "30 seconds") \
       .option('truncate', False) \
       .start()
    join_query.awaitTermination()
    


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    # Create Spark in Standalone mode
    spark = SparkSession \
        .builder \
        .master("local[16]") \
        .config("spark.ui.port", 3000) \
        .config("spark.sql.streaming.metricsEnabled", "true") \
        .config('spark.driver.memory', '3g') \
        .appName("KafkaSparkStructuredStreaming") \
        .getOrCreate()
     
    spark.sparkContext.setLogLevel("info")
    logger.info("Spark started")

    run_spark_job(spark)

    spark.stop()
