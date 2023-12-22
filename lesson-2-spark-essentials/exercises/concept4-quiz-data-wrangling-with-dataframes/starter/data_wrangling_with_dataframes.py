# # Data Wrangling with DataFrames Coding Quiz
import findspark
findspark.init('/opt/homebrew/Cellar/apache-spark/3.5.0/libexec')

from pyspark.sql import SparkSession
import pandas as pd

sc = SparkSession.builder.appName("Sparkify").getOrCreate()

path = "lesson-2-spark-essentials/exercises/data/sparkify_log_small.json"
user_log = sc.read.json(path)

# TODOS: 
# 1) import any other libraries you might need
# 2) instantiate a Spark session 
# 3) read in the data set located at the path "../../data/sparkify_log_small.json"
# 4) write code to answer the quiz questions 


# # Question 1
# 
# Which page did user id "" (empty string) NOT visit?

visited_pages = user_log.select("page").where(user_log.userId == "").dropDuplicates()
distinct_pages = user_log.select("page").distinct()

distinct_pages[~distinct_pages.page.isin(visited_pages.page)].show()

# # Question 2 - Reflect
# 
# What type of user does the empty string user id most likely refer to?
# 


# TODO: use this space to explore the behavior of the user with an empty string


# # Question 3
# 
# How many female users do we have in the data set?


# TODO: write your code to answer question 3


# # Question 4
# 
# How many songs were played from the most played artist?

# TODO: write your code to answer question 4

# # Question 5 (challenge)
# 
# How many songs do users listen to on average between visiting our home page? Please round your answer to the closest integer.
# 
# 

# TODO: write your code to answer question 5

