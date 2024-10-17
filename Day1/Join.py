# Databricks notebook source
df_sales=spark.table("sales")

# COMMAND ----------

df_customer=spark.table("customers_spark_sql")

# COMMAND ----------

df_joined=df_sales.join(df_customer,"customer_id")


# COMMAND ----------

df_joined = df_sales.join(df_customer,df_sales["customer_id"]==df_customer["customer_id"],"inner")

# COMMAND ----------

df_joined.display()

# COMMAND ----------

df_customer.display()

# COMMAND ----------

df_customer.filter("customer_id=2").display()

# COMMAND ----------

#from pyspark.sql.functions import *
df_customer.where(col("customer_id") == 2).display()

# COMMAND ----------

df_customer.where("customer_id>2 or customer_city='New Michaelview'").display()

# COMMAND ----------

#df_customer.sort("customer_name").display()
#df_customer.sort("customer_city",ascending=True).display()
df_customer.sort(col("customer_city").desc()).display()

# COMMAND ----------

#df_joined = df_sales.join(df_customer,df_sales["customer_id"]==df_customer["customer_id"],"inner")
df_joined.groupBy(df_sales["customer_id"]).count().display()
#if the names are different then mention it as above


# COMMAND ----------

df_joined.count()

# COMMAND ----------

df_joined = df_sales.join(df_customer, "customer_id","inner")

# COMMAND ----------

df_joined.groupBy("customer_id").count().display()
