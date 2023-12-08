###
# You might have noticed this code in the screencast.
#
import findspark
findspark.init('/opt/homebrew/Cellar/apache-spark/3.5.0/libexec')
#

import pyspark
from pyspark import SparkConf
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Our first Python Spark SQL example") \
    .getOrCreate()

## READ DATA

path = "../../data/sparkify_log_small.json"
user_log = spark.read.json(path)
user_log.printSchema()
user_log.describe()
user_log.show(n=1)
user_log.take(5)

## WRITE DATA
out_path = "../../data/sparkify_log_small.csv"
user_log.write.save(out_path, format="csv", header=True)
user_log_2 = spark.read.csv(out_path, header=True)
user_log_2.printSchema()
user_log_2.take(2)
user_log_2.select("userID").show()
user_log_2.take(1)
