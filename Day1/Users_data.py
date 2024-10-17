# Databricks notebook source
from pyspark.sql import Row
import datetime

users=[
    {
        "id":1,
        "name":"Sachin",
        "last_name":"Tendulkar",
        "email":"sachin@gmail.com",
        "Mobile":Row(mobile= "342779900", home= "91568557700"),
        "courses": [1,2],
        "is_customer":True,
        "DOB": datetime.date(1973,4,24)
    },
    {
         "id":2,
        "name":"Virat",
        "last_name":"Kohli",
        "email":"virat@gmail.com",
        "Mobile":Row(mobile= "91556600", home= "918912300"),
        "courses": [2,3],
        "is_customer":True, 
        "DOB":datetime.date(1988,11,5)
    },
     {
         "id":3,
        "name":"Rohit",
        "last_name":"Sharma",
        "email":"rohit@gmail.com",
        "Mobile":Row(mobile= "914455700", home= "9145997700"),
        "courses": [3],
        "is_customer":True, 
        "DOB":datetime.date(1987,4,30)
     },
     {
         "id":4,
        "name":"Dinesh",
        "last_name":"Karthik",
        "email":"dinesh@gmail.com",
        "Mobile":Row(mobile= "91467700", home= "916789700"),
        "courses": [3],
        "is_customer":True, 
        "DOB":datetime.date(1985,6,1)
     },
     {
         "id":5,
        "name":"M S",
        "last_name":"Dhoni",
        "email":"dhoni@gmail.com",
        "Mobile":Row(mobile= "91467799", home= "916778800"),
        "courses": [3],
        "is_customer":True, 
        "DOB":datetime.date(1981,7,7)
     }
]

# COMMAND ----------

df_users = spark.createDataFrame(users)

# COMMAND ----------

df_users.display()

# COMMAND ----------


from pyspark.sql.functions import *


# COMMAND ----------

# MAGIC %md
# MAGIC 1. rename id cl to emp id
# MAGIC 2. to new col with current_timestap
# MAGIC 3. drop is_customer col
# MAGIC 4. concat name and last_name

# COMMAND ----------


df_users.select(col("id").alias("emp_id")).display()

# COMMAND ----------

df_users.withColumn("date",current_date()).display()

# COMMAND ----------

df_users\
.withColumnRenamed("id","emp_id")\
.withColumn("ingestion_date", current_timestamp())\
.drop("is_customer").display()

# COMMAND ----------

df_users\
.withColumnRenamed("id","emp_id")\
.withColumn("ingestion_date", current_timestamp())\
.withColumn("full_name",concat("name",lit(" "),"last_name"))\
.drop("is_customer","name","last_name")\
.display()

# COMMAND ----------

df_users_final = df_users\
.withColumnRenamed("id","emp_id")\
.withColumn("ingestion_date", current_timestamp())\
.withColumn("full_name",concat("name",lit(" "),"last_name"))\
.drop("is_customer","name","last_name")


# COMMAND ----------

(df_users
.withColumnRenamed("id","emp_id")
.withColumn("ingestion_date", current_timestamp())
.drop("is_customer"))

# COMMAND ----------

df_users_final.display()

# COMMAND ----------

df_users\
.withColumnRenamed("id","emp_id")\
.withColumn("ingestion_date",current_timestamp())\
.withColumn("full_name",concat("name",lit(" "),"last_name"))\
.drop("is_customer","name","last_name")\
    .display()

# COMMAND ----------

data = {"id":1, "name":"a","mo":123}
data = {"id":1, "name":"a","mo":[123,888]}
data = {"id":1, "name":"a","mo":{"home":123,"office":7700}}

# COMMAND ----------

df_users_final.withColumn("courses",explode("courses")).display()

# COMMAND ----------

df_users_final.withColumn("mobile_phone",col("Mobile.mobile"))\
    .withColumn("mobile_home",col("Mobile.home"))\
    .drop("Mobile")\
.display()

# COMMAND ----------

df_final = df_users_final.withColumn("mobile_phone",col("Mobile.mobile"))\
    .withColumn("mobile_home",col("Mobile.home"))\
    .drop("Mobile")\
        .withColumn("courses",explode("courses"))\
        .display()


# COMMAND ----------



# COMMAND ----------


