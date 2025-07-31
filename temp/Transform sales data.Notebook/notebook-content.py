# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "49abe238-143d-41f7-bd14-b0a7c9629c23",
# META       "default_lakehouse_name": "lh_sales",
# META       "default_lakehouse_workspace_id": "acba7f5a-911c-4f9a-afe8-3a5da7f4e3c9",
# META       "known_lakehouses": [
# META         {
# META           "id": "49abe238-143d-41f7-bd14-b0a7c9629c23"
# META         }
# META       ]
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

table_name= 'sales_using_copydata'


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import *

df_sales = spark.read.format("csv").option("header" , "true").load("Files/new_data/*.csv")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


df_sales = df_sales.withColumn("OrderYear", year(col=("OrderDate")))\
                   .withColumn("OrderMonth",month(col("OrderDate")))\
                   .withColumn("FirstName" , split(col("CustomerName")," ").getItem(0))\
                   .withColumn("LastName",split(col("CustomerName")," " ).getItem(1))

df_sales = df_sales["SalesOrderNumber" , "SalesOrderLineNumber","OrderDate","OrderYear","OrderMonth","FirstName","LastName","EmailAddress","Item","Quantity","UnitPrice","TaxAmount"]

df_sales .write.format("delta").mode("append").saveAsTable(table_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
