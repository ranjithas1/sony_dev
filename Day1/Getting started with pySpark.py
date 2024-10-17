# Databricks notebook source
print("hello")

# COMMAND ----------

# MAGIC %md
# MAGIC spark core.
# MAGIC
# MAGIC RDD DataFrame

# COMMAND ----------

# DBTITLE 1,DataFrame : Structured API
data=[(1, 'a', 20), (2, 'b', 30)]
schema=["id", "name", "age"]
df = spark.createDataFrame(data,schema)
df.display()

# COMMAND ----------

data=[(1, 'a', 20), (2, 'b', 30)]
schema="id int, name string, age int"
df = spark.createDataFrame(data,schema)
df.display()

# COMMAND ----------

df = spark.createDataFrame(data)

# COMMAND ----------

df.show();

# COMMAND ----------

df.display()


# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC dataframe functions
# MAGIC .select
# MAGIC .display
# MAGIC .alias
# MAGIC .withcolumnrenamed
# MAGIC .withcolumnsrenamed
# MAGIC
# MAGIC functions
# MAGIC .col

# COMMAND ----------

df.select("*")

# COMMAND ----------

df.select("*").display()

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

df1 = df.select("id","age")

# COMMAND ----------

df1.display()

# COMMAND ----------



# COMMAND ----------

df1.display()

# COMMAND ----------



# COMMAND ----------

df.select("id".alias("emp_id"))

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df.select(col("id").alias("emp_id")).display()

# COMMAND ----------

help(df.withColumnRenamed)

# COMMAND ----------

df.withColumnRenamed("id","emp_id").display()

# COMMAND ----------

df.withColumnsRenamed({"id":"emp_id","name":"emp_name","age":"emp_age"}).display()
df_new = df.withColumnsRenamed({"id":"emp_id","name":"emp_name","age":"emp_age"})

# COMMAND ----------

df_new.display()

# COMMAND ----------

df.withColumn("current_date",current_date()).display()

# COMMAND ----------

df.withColumn("age",current_date()).display()

# COMMAND ----------

df.withColumn("date",current_date()).display()
