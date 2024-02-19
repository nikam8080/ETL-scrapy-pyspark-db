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

    # csv_file_path = "/Users/nikhilnikam/Downloads/Audio_Data_ETL/scrapy/Audio_Data/output.csv"

    # Defined schema
    flipkartAudioSchema = StructType([
        StructField("audio_name", StringType()),
        StructField("formatted_actual_price", IntegerType(), nullable=False),
        StructField("offer_price", IntegerType()),
        StructField("reviews_count", IntegerType()),
        StructField("audio_type", StringType())
    ])


    flipkartAudioDataDF = spark.read \
        .format("csv") \
        .option("header", "true") \
        .schema(flipkartAudioSchema) \
        .csv("/Users/nikhilnikam/Downloads/Audio_Data_ETL/scrapy/Audio_Data/output.csv")

    flipkartAudioDataDF.printSchema()
    flipkartAudioDataDF.show()
    logger.info("csv Schema:" +flipkartAudioDataDF.schema.simpleString())


    # read_df = spark.read \
    #     .csv(csv_file_path, header=True ,inferSchema=True)

    # changed_datatype_df = read_df \
    #     .withColumn("formatted_actual_price", col("formatted_actual_price").cast("int")) \
    #     .withColumn("offer_price", col("offer_price").cast("int")) \
    #     .withColumn("reviews_count", col("reviews_count").cast("int"))

    # invalid_rows_df = changed_datatype_df.filter(col("formatted_actual_price").isNull() | 
    #                                       col("offer_price").isNull() | 
    #                                       col("reviews_count").isNull())

    # invalid_rows_df.printSchema()
    # invalid_rows_df.show()

    # datatype_df = read_df.withColumn("formatted_actual_price", col("formatted_actual_price").cast("integer"))

    # datatype_df.printSchema()
    # datatype_df.show()
    # column_name = "offer_price"

    # read_df.select(column_name).show()

    # remove_symbol_df = flipkartAudioDataDF \
    #     .withColumn("offer_price", regexp_replace("offer_price", ",", "")) \
    #     .withColumn("formatted_actual_price", regexp_replace("formatted_actual_price", ",", "")) \
    #     .withColumn("reviews_count", regexp_replace("reviews_count", "[\(\),]", ""))
    # remove_symbol_df.show()
    # remove_symbol_df.printSchema()



    # datatype_df = remove_symbol_df \
    #     .withColumn("formatted_actual_price", col("formatted_actual_price").cast("integer"))
    # datatype_df.printSchema()
    # datatype_df.show()




    # datatype_df = read_df.withColumn("formatted_actual_price", regexp_replace(col("formatted_actual_price"), "₹", "")) \
    #     .withColumn("formatted_actual_price", regexp_replace(col("formatted_actual_price"), ",", "")) \
    #     .withColumn("offer_price", regexp_replace(col("offer_price"), ",", "")) \
    #     .withColumn("reviews_count", regexp_replace("reviews_count", "[\(\),]", ""))
    #     # .withColumn("offer_price", regexp_replace(col("offer_price"), "₹", "")) \


        # .withColumn("reviews_count", regexp_replace(col("reviews_count"), "(", "")) \
        # .withColumn("reviews_count", regexp_replace(col("reviews_count"), ",", "")) 
        # .withColumn("reviews_count", regexp_extract(col("reviews_count"), ")", ""))

    # Remove commas from reviews_count and cast it to integer
    # datatype_df = datatype_df.withColumn("reviews_count", regexp_replace(col("reviews_count"), ",", "").cast("integer"))

    # Cast price columns to integer
    # datatype_df = datatype_df \
    #     .withColumn("formatted_actual_price", col("formatted_actual_price").cast("integer")) \
    #     .withColumn("offer_price", col("offer_price").cast("integer")) \
    #     .withColumn("reviews_count", col("reviews_count").cast("integer"))

    # datatype_df.printSchema()
    # datatype_df.show()

    