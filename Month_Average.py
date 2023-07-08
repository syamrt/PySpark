# Databricks notebook source
from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("Avg")
sc = SparkContext.getOrCreate(conf=conf)
textfile = sc.textFile('/FileStore/tables/average_quiz_sample.csv')

# COMMAND ----------

textfile.map(lambda x: (x.split(',')[0], ( float(x.split(',')[2]), 1))).reduceByKey(lambda x,y: (x[0]+y[0],x[1]+y[1])).map(lambda x: (x[0],x[1][0]/x[1][1])).collect()

# COMMAND ----------

movieratings = sc.textFile('/FileStore/tables/movie_ratings.csv')

# COMMAND ----------

movieratings.map(lambda x: x.split(',')).groupByKey().mapValues(list).map(lambda x: (x[0],min(x[1]), max(x[1]))).collect()

# COMMAND ----------

movieratings.map(lambda x: x.split(',')).reduceByKey(lambda x,y: x if x<y else y).collect()

# COMMAND ----------

movieratings.map(lambda x: x.split(',')).reduceByKey(lambda x,y: x if x>y else y).collect()

# COMMAND ----------

textfile.map(lambda x: (x.split(',')[1], float(x.split(',')[2]))).groupByKey().mapValues(list).map(lambda x: (x[0], min(x[1]), max(x[1]))).collect()

# COMMAND ----------


