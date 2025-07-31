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

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Sales Order Data Exploration


# CELL ********************

df = spark.read.format("csv").option("header","false").load("Files/orders (2)/2019.csv")
# df now is a Spark DataFrame containing CSV data from "Files/orders (2)/2019.csv".
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import *

order_schema = StructType([
                StructField("sales_order_nember" , StringType()) ,
                StructField("sales_order_line_number", IntegerType()) ,
                StructField("order_date", DateType()),
                StructField("customer_name" , StringType()),
                StructField("email",StringType()),
                StructField("item",StringType()),
                StructField("quantity", IntegerType()),
                StructField("unit_price", FloatType()),
                StructField("tax", FloatType())
])

df= spark.read.format("csv").schema(order_schema).load("Files/orders (2)/*.csv")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import StructType , StructField, IntegerType , StringType, DateType, DoubleType

product_schema = StructType([
        StructField("product_id",IntegerType()),
        StructField("product_name", StringType()),
        StructField("category",StringType()),
        StructField("list_price", DoubleType())
])

df_product = spark.read.format("csv").option("header" , "true").schema(product_schema).load("Files/products/products.csv")

display(df_product)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_product.write.format("delta").saveAsTable("managed_products")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_product.write.format("delta").saveAsTable("external_product",path="abfss://ws_DP600@onelake.dfs.fabric.microsoft.com/lh_sales.Lakehouse/Files/external_products")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC DESCRIBE FORMATED managed_products

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC DESCRIBE   external_products;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC DROP TABLE external_products;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
