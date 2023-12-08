# Take care of any imports
import findspark
findspark.init('/opt/homebrew/Cellar/apache-spark/3.5.0/libexec')

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import desc
from pyspark.sql.functions import asc
from pyspark.sql.functions import sum as Fsum

import datetime

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# Create the Spark Context
spark = SparkSession.builder.appName("DataWrangler").getOrCreate()


# Complete the script

path = "../../data/sparkify_log_small.json"
user_log = spark.read.json(path)

# # Data Exploration 
# 
# # Explore the data set.


# View 5 records 
user_log.take(5)
# Print the schema
user_log.printSchema()

# Describe the dataframe
user_log.describe().show()

# Describe the statistics for the song length column
user_log.describe("length").show()

# Count the rows in the dataframe
print(user_log.count())

# Select the page column, drop the duplicates, and sort by page
user_log.select("page").dropDuplicates().show()

# Select data for all pages where userId is 1046
user_log.select("page").where(user_log.userId == "1046").show()

# # Calculate Statistics by Hour
get_hour = udf(lambda x: datetime.datetime.fromtimestamp(x / 1000.0).hour)
user_log = user_log.withColumn("hour", get_hour(user_log.ts))
songs_in_hour = (user_log.filter(user_log.page == "NextSong").groupby(user_log.hour).count()
                 .orderBy(user_log.hour.cast("float")))
songs_in_hour.show()

songs_in_hour_pd = songs_in_hour.toPandas()
songs_in_hour_pd.hour = pd.to_numeric(songs_in_hour_pd.hour)

plt.scatter(songs_in_hour_pd["hour"], songs_in_hour_pd["count"])
plt.xlim(-1, 24)
plt.ylim(0, 1.2 * max(songs_in_hour_pd["count"]))
plt.xlabel("Hour")
plt.ylabel("Songs played")
plt.show()

# Select just the NextSong page
user_log.where(user_log.page == 'NextSong').show()

# # Drop Rows with Missing Values
user_log_valid = user_log.dropna(how = "any", subset = ["userId", "sessionId"])

# How many are there now that we dropped rows with null userId or sessionId?
print(user_log_valid.count())

# select all unique user ids into a dataframe
user_log.select("UserId").dropDuplicates().sort("UserId").show()

# Select only data for where the userId column isn't an empty string (different from null)
user_log_valid = user_log_valid.filter(user_log_valid["userId"] != "")
print(user_log_valid.count())
# # Users Downgrade Their Accounts
#
# Find when users downgrade their accounts and then show those log entries. 
user_log_valid.filter("page = 'Submit Downgrade'").show()
user_log_valid.select(["userId", "firstname", "page", "level", "song"]) \
    .where(user_log_valid.userId == "1138") \
    .show()


# Create a user defined function to return a 1 if the record contains a downgrade
flag_downgrade_event = udf(lambda x: 1 if x == "Submit Downgrade" else 0, IntegerType())


# Select data including the user defined function
user_log_valid = user_log_valid \
    .withColumn("downgraded", flag_downgrade_event("page"))

print(
    user_log_valid.head()
)



# Partition by user id
# Then use a window function and cumulative sum to distinguish each user's data as either pre or post downgrade events.
from pyspark.sql import Window
windowval = Window.partitionBy("userId").orderBy(desc("ts")).rangeBetween(Window.unboundedPreceding, 0)


# Fsum is a cumulative sum over a window - in this case a window showing all events for a user
# Add a column called phase, 0 if the user hasn't downgraded yet, 1 if they have
user_log_valid = user_log_valid.withColumn("phase", Fsum("downgraded").over(windowval))
user_log_valid.show()


# Show the phases for user 1138 
(user_log_valid.select(["userId", "firstname", "ts", "page", "level", "phase"])
 .where(user_log_valid.userId == "1138").sort("ts", ascending=True).show())
