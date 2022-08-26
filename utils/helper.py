from pyspark.sql import SparkSession
import json
import datetime

timestamp = datetime.datetime.now().strftime("%Y_%m_%d_%H_%M_%S")


def create_spark_session(master="local[1]", appName = timestamp):
    # Enabling Hive to use in Spark
    spark = SparkSession.builder \
        .master(master) \
        .appName(appName) \
        .getOrCreate()
    return spark


def json_parser():
    f = open('../config/file.json')
    data = json.load(f)
    return data