# Databricks notebook source
from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("StudentDataAnalysis")
sc = SparkContext.getOrCreate(conf=conf)


# COMMAND ----------

student_data = sc.textFile("/FileStore/tables/StudentData.csv")

# COMMAND ----------

header = student_data.first()
student_data = student_data.filter(lambda x: x!=header)

# COMMAND ----------

student_data.count()

# COMMAND ----------

student_data.collect()

# COMMAND ----------

student_data.map(lambda x: (x.split(',')[1], int(x.split(',')[5]))).reduceByKey(lambda x,y: x+y).collect()

# COMMAND ----------

student_data.map(lambda x: x.split(',')[5]).map(lambda x: (['P' if int(x)>50 else 'F']),1).map(lambda x: (x[0],1)).reduceByKey(lambda x,y: x+y).collect()

# COMMAND ----------

student_data.filter(lambda x: int(x.split(',')[5])>50).count()


# COMMAND ----------

student_data.filter(lambda x: int(x.split(',')[5])<=50).count()

# COMMAND ----------

student_data.map(lambda x: (x.split(',')[3],1)).reduceByKey(lambda x,y: x+y).collect()

# COMMAND ----------

student_data.map(lambda x: (x.split(',')[3],int(x.split(',')[5]))).reduceByKey(lambda x,y: x+y).collect()

# COMMAND ----------

student_data.map(lambda x: (x.split(',')[3],(int(x.split(',')[5]),1))).reduceByKey(lambda x,y: (x[0]+y[0],x[1]+y[1])).map(lambda x: (x[0],x[1][0]/x[1][1])).collect()

# COMMAND ----------

student_data.map(lambda x: (x.split(',')[3],(int(x.split(',')[5]),1))).reduceByKey(lambda x,y: (x[0]+y[0],x[1]+y[1])).mapValues(lambda x: x[0]/x[1]).collect()

# COMMAND ----------

student_data.map(lambda x: (x.split(',')[3],int(x.split(',')[5]))).groupByKey().mapValues(list).map(lambda x: [x[0], min(x[1]), max(x[1])]).collect()

# COMMAND ----------

student_data.map(lambda x: (x.split(',')[1], (int(x.split(',')[0]), 1))).reduceByKey(lambda x,y: (x[0]+y[0],x[1]+y[1])).map(lambda x: [x[0], x[1][0]/x[1][1]] ).collect()

# COMMAND ----------


