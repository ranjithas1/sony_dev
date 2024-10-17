# Databricks notebook source
# MAGIC %sql
# MAGIC select * from file_format.'path'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM json.`/Volumes/databricks_ranjitha/default/raw/customers.json`

# COMMAND ----------

# MAGIC %sql
# MAGIC create table customers_spark_sql as 
# MAGIC select *, current_timestamp() as ingestion_date from json.`/Volumes/databricks_ranjitha/default/raw/customers.json`

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM json.`/Volumes/databricks_ranjitha/default/raw/products.json`

# COMMAND ----------

# MAGIC %sql
# MAGIC create table products_spark_sql as 
# MAGIC select *, current_timestamp() as ingestion_date from json.`/Volumes/databricks_ranjitha/default/raw/products.json`

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customers_spark_sql

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customers_spark_sql where customer_id=2
