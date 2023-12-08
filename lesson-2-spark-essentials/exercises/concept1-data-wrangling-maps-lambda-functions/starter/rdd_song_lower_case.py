### 
# You might have noticed this code in the screencast.
#
import findspark
findspark.init('/opt/homebrew/Cellar/apache-spark/3.5.0/libexec')
#
# The findspark Python module makes it easier to install
# Spark in local mode on your computer. This is convenient
# for practicing Spark syntax locally. 
# However, the workspaces already have Spark installed and you do not
# need to use the findspark module
#
###

from pyspark.sql import SparkSession

# Because we aren't running on a spark cluster, the session is just for development
spark = SparkSession \
    .builder \
    .appName("Maps and Lazy Evaluation Example") \
    .getOrCreate()

# Starting off with a regular python list
log_of_songs = [
        "Despacito",
        "Nice for what",
        "No tears left to cry",
        "Despacito",
        "Havana",
        "In my feelings",
        "Nice for what",
        "despacito",
        "All the stars"
]

# parallelize the log_of_songs to use with Spark
dist_log_of_songs = spark.sparkContext.parallelize(log_of_songs)

# show the original input data is preserved
dist_log_of_songs.foreach(print)

# create a python function to convert strings to lowercase
def convert_song_to_lowercase(song):
    return song.lower()

print(convert_song_to_lowercase("Songtitle"))

# use the map function to transform the list of songs with the python function that converts strings to lowercase
dist_log_of_songs.map(convert_song_to_lowercase)

# Show the original input data is still mixed case
dist_log_of_songs.foreach(print)

# Use lambda functions instead of named functions to do the same map operation
dist_log_of_songs.map(lambda song: song.lower())