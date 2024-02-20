from asyncio.log import logger
from asyncore import read
import imp
from operator import imod
from optparse import Option
from struct import Struct
import turtle
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

from lib.logger import setup_logging

if __name__ == "__main__":

    setup_logging()

    spark = SparkSession \
        .builder \
        .master("local[3]") \
        .appName("FlipkartDataProc") \
        .getOrCreate()

    csv_file_path = "/Users/nikhilnikam/Downloads/Audio_Data_ETL/scrapy/Audio_Data/output.csv"

    # Defined schema
    # flipkartAudioSchema = StructType([
    #     StructField("audio_name", StringType()),
    #     StructField("formatted_actual_price", IntegerType(), nullable=False),
    #     StructField("offer_price", IntegerType()),
    #     StructField("reviews_count", IntegerType()),
    #     StructField("audio_type", StringType())
    # ])


    flipkartAudioDataDF = spark.read \
        .format("csv") \
        .option("header", "true") \
        .csv(csv_file_path)

    
    logger.info("csv Schema:" +flipkartAudioDataDF.schema.simpleString())


    datatype_df = flipkartAudioDataDF.withColumn("formatted_actual_price", regexp_replace(col("formatted_actual_price"), "₹", "")) \
        .withColumn("formatted_actual_price", regexp_replace(col("formatted_actual_price"), ",", "")) \
        .withColumn("offer_price", regexp_replace(col("offer_price"), ",", "")) \
        .withColumn("reviews_count", regexp_replace("reviews_count", "[\(\),]", "")) \
        .withColumn("offer_price", regexp_replace(col("offer_price"), "₹", ""))


    changed_datatype_df = datatype_df \
        .withColumn("formatted_actual_price", col("formatted_actual_price").cast("int")) \
        .withColumn("offer_price", col("offer_price").cast("int")) \
        .withColumn("reviews_count", col("reviews_count").cast("int"))



    changed_datatype_df.show()
    changed_datatype_df.printSchema()

    spark.stop()










    
    