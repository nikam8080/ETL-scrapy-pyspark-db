from asyncore import read
import imp
from optparse import Option
import turtle
from pyspark.sql import *

if __name__ == "__main__":

    spark = SparkSession \
        .builder \
        .master("local[3]") \
        .appName("FlipkartDataProc") \
        .getOrCreate()

    csv_file_path = "/Users/nikhilnikam/Downloads/Audio_Data_ETL/scrapy/Audio_Data/output.csv"

    read_df = spark.read \
        .csv(csv_file_path, header=True ,inferSchema=True)

    read_df.printSchema()
    read_df.show()

    