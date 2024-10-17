# Databricks notebook source
--pyspark df
--Spark SQL

Extract
data format (csv, table, json, parquet, delta, avro, orc)
(ADLS, DAtabases, DW, S3)


Transform

L (csv, son, parquet, delta)

Load(parquet, DELTA) - its a big data file format - delta is a extension of parquet which has more features
-- its a compressed version
-- cannot open ann download
--snowflake default format is iceburg
--databricks default file format is delta


# COMMAND ----------

csv(1gb)  - reduce to 60-80% when it comes to parque

parquet() -- always recommend parque to reduce the size - size will be 200-300mb data

# COMMAND ----------

df=spark.read.csv("/Volumes/databricks_ranjitha/default/raw/sales.csv",header=True,inferSchema=True)


# COMMAND ----------

df.display()

# COMMAND ----------

df_products = spark.read.json("/Volumes/databricks_ranjitha/default/raw/products.json")

# COMMAND ----------

df_products.display()

# COMMAND ----------

df_customer = spark.read.json("/Volumes/databricks_ranjitha/default/raw/customers.json")

# COMMAND ----------

df_customer.display()

# COMMAND ----------

df_customer.write.parquet("path")

# COMMAND ----------

df_customer.write.saveAsTable("customer")

# COMMAND ----------

df.write.saveAsTable("sales")

# COMMAND ----------

df.write.mode("overwrite").saveAsTable("sales")
