# Databricks notebook source
# MAGIC %run /Workspace/Users/ranjithasuresh1@outlook.com/Day1/includes

# COMMAND ----------

df_sales=spark.read.csv(f"{input_path}order_dates.json",header=True,inferSchema=True)

# COMMAND ----------

df1=add_ingestion(df_sales)

# COMMAND ----------

df1.write.mode("overwrite").saveAsTable("order_dates")

# COMMAND ----------

df1.display()
