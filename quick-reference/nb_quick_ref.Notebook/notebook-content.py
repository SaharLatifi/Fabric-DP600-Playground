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

import pandas as pd

wrangler_sample_df = pd.read_csv("https://aka.ms/wrangler/titanic.csv")
display(wrangler_sample_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "editable": true
# META }

# MARKDOWN ********************

# # **Load data from file**

# MARKDOWN ********************

# **Inferring a schema**  


# CELL ********************

df = spark.read.format('csv').load('Files/sales_data/sales.csv')
display(df.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Explicit schema**

# CELL ********************

from pyspark.sql.types import *
from pyspark.sql.functions import *

sales_schema = StructType([
        StructField("SalesOrderNumber" , StringType()),
        StructField("SalesOrderLineNumber" , IntegerType()) ,
        StructField("OrderDate" , DateType()) ,
         StructField("CustomerName" , StringType()),
        StructField("EmailAddress" , StringType()),
         StructField("Item" , StringType()) ,
        StructField("Quantity",IntegerType())
])

df=spark.read.format('csv').schema(sales_schema).option("header","true").load('Files/sales_data/sales.csv')
df_with_year = df.withColumn("OrderYear" , year(df["OrderDate"]))
display(df_with_year.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # **Filter a datafram - by row and column**

# CELL ********************

# Filter by column
df_filtered_by_col1 = df_with_year.select("CustomerName" , "EmailAddress")
df_filtered_by_col2 = df_with_year["CustomerName" , "EmailAddress"]


#Filter by row
df_filtered_by_row_1 = df_with_year.filter(df_with_year["CustomerName"] == "Julio Ruiz")
df_filtered_by_row_2 = df_with_year.where(df_with_year["CustomerName"] == "Emma Brown")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Filter by column using Select or []

filtered_df_using_select = df_with_year.select("CustomerName","EmailAddress")

filtered_df = df_with_year["CustomerName","EmailAddress"]
print(f"Number of rows {filtered_df.count()}")
print(f"Distinct rows {filtered_df.distinct().count()}")
display(filtered_df)
display(filtered_df.distinct())


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Filter by column and row
df_filtered_by_col_row  = df_with_year["CustomerName","EmailAddress"].where(col("CustomerName").like("%Jan%"))
df_filtered_by_col_row_1 = df_with_year["CustomerName","EmailAddress"].filter(col("CustomerName").like("%Jan%"))
display(df_filtered_by_col_row)
display(df_filtered_by_col_row_1)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # **Aggregate and Group a dataframe**

# CELL ********************

# Different ways to do this
df_sum = df_with_year.groupBy('CustomerName').agg({'Quantity': 'sum'})
df_sum = df_with_year.select("CustomerName" , "Quantity").groupBy("CustomerName").sum()
df_yearly_sales = df.select(year(col("OrderDate")).alias("Year")).groupBy("Year").count().orderBy("Year")
df_grouped = df_with_year.select("CustomerName","OrderYear","Quantity").groupBy("CustomerName","OrderYear").sum("Quantity").withColumnRenamed("sum(Quantity)","sum_quantity")
display(df_sum)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # **Using Spark to transform data**

# CELL ********************

# add columns
transformed_df = df.withColumn("Year" , year(col("OrderDate"))) \
                   .withColumn("Month" , month(col("OrderDate"))) \
                   .withColumn("FirstName" , split(col("CustomerName")," ").getItem(0)) \
                   .withColumn("LastName" , split(col("CustomerName"), " ").getItem(1))
# filter and reorder columns
transformed_df = transformed_df["SalesOrderNumber" ,"SalesOrderLineNumber","OrderDate" , "Year","Month","FirstName","LastName","EmailAddress","Item","Quantity"]

# display the first 5 rows
display(transformed_df.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # **Saving a dataframe  - as a file in Files folder**  


# CELL ********************

transformed_df.write.mode("overwrite").parquet("Files/transformed_data/sales")
read_from_transfromed_data_df = spark.read.format("parquet").load("Files/transformed_data/sales")
display(read_from_transfromed_data_df)
# Partition the data
df_grouped.write.mode("overwrite").partitionBy("OrderYear").parquet("Files/sales_data/sales_grouped")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Loading partitioned data**

# CELL ********************

df_sales_grouped = spark.read.parquet("Files/sales_data/sales_grouped/OrderYear=2021")
#display(df_sales_grouped)
number_of_rows_2021 = df_sales_grouped.count()
df_sales_grouped = spark.read.parquet("Files/sales_data/sales_grouped")
number_of_rows_all = df_sales_grouped.count()
print(f"Number Of Rows - 2021: {number_of_rows_2021} , Number of Rows - All:  {number_of_rows_all}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # **Work with delta table** 

# MARKDOWN ********************

# **Using Spark SQL / SQL**

# MARKDOWN ********************

# Create a temporary view - automatically deleted at the end of the current session.  
# We can also create tables that are persisted in the catalog to define a database that an be queries using sparkSQL

# CELL ********************

df_sales_grouped.createOrReplaceTempView("sales_grouped_view")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- Create a temporary view
# MAGIC CREATE OR REPLACE TEMPORARY VIEW vw_sales_grouped
# MAGIC AS
# MAGIC     SELECT CustomerName , Item , SUM(Quantity * UnitPrice)  AS TotalSales
# MAGIC     FROM  sales_data
# MAGIC     GROUP BY CustomerName , Item
# MAGIC     HAVING SUM(Quantity) > 1 


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Visualize the data**

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT *
# MAGIC FROM vw_sales_grouped 
# MAGIC ORDER BY TotalSales DESC 
# MAGIC LIMIT 10


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import  col, desc
df_sales_by_customer = spark.sql("SELECT * FROM vw_sales_grouped").orderBy(col("TotalSales").desc())
display(df_sales_by_customer)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC --select * from sales_grouped_view
# MAGIC UPDATE sales_data
# MAGIC SET  Quantity= 50 WHERE CustomerName = 'Kelvin Huang';

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT *
# MAGIC FROM sales_data
# MAGIC WHERE CustomerName = 'Kelvin Huang'

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Query the data using Spqrk SQL API

# CELL ********************

df = spark.sql("SELECT * FROM sales_grouped")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ** **Use Delta API**
# When you want to work with delta files rather than catalog tables, it may be simpler to use the Delta Lake API. You can create an instance of a DeltaTable from a folder location containing files in delta format, and then use the API to modify the data in the table

# MARKDOWN ********************

# # Create delta table from a dataframe

# CELL ********************

df_sales_grouped.write.format("delta").saveAsTable("sales_grouped")
# writing as a file : df_grouped.write.mode("overwrite").partitionBy("OrderYear").parquet("Files/sales_data/sales_grouped")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import *
from pyspark.sql.functions import *

# Create a DeltaTable object
delta_path = "Files/mytable"
deltaTable = DeltaTable.forPath(spark, delta_path)

# Update the table (reduce price of accessories by 10%)
deltaTable.update(
    condition = "CustomerName == 'Kelvin Huang'",
    set = { "Quantity": "Quantity * 3" })

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Get the table description**

# CELL ********************

spark.sql("DESCRIBE EXTENDED sales_grouped").show(truncate=  False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Create external table**  
# Metadata will be defined in the catalog, but  the underlying data may be stored somewhere other than the lakehouse, with the schema metadata stored in the lakehouse.
# 
# Deleting an external table doesn't delete the underlying data. Metadata(table schema, location reference) is removed from the metastore/catalog.  
# So the data still physically exists in the storage location and by creating a new external table pointing to the same location or querying the data using raw file access the data can be accessed.  
# In the Explorer pane, in the … menu for the Files folder, select Copy ABFS path. The ABFS path is the fully qualified path to the lakehouse Files folder.  
# The full path should look similar to this:  
# abfss://workspace@tenant-onelake.dfs.fabric.microsoft.com/lakehousename.Lakehouse/Files/external_products


# CELL ********************

#spark.catalog.createExternalTable
df_sales_grouped.write.format("delta").saveAsTable("sales_grouped_external", path="Files/sales_grouped_external")
# specify a fully qualified path for a storage location, like this
df_sales_grouped.write.format("delta").saveAsTable("sales_grouped_external", path="abfss://my_store_url..../sales_grouped_external")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Create table metadata**  
# While it's common to create a table from existing data in a dataframe, there are often scenarios where you want to create a table definition in the metastore that will be populated with data in other ways. There are multiple ways you can accomplish this goal.

# CELL ********************

# Using DeltaTableBuilder API 
from delta.tables import *

DeltaTable.create(spark) \
  .tableName("products") \
  .addColumn("Productid", "INT") \
  .addColumn("ProductName", "STRING") \
  .addColumn("Category", "STRING") \
  .addColumn("Price", "FLOAT") \
  .execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Using SQL
%%sql

# Managed table
CREATE TABLE salesorders
(
    Orderid INT NOT NULL,
    OrderDate TIMESTAMP NOT NULL,
    CustomerName STRING,
    SalesTotal FLOAT NOT NULL
)
USING DELTA

#External table

CREATE TABLE MyExternalTable
USING DELTA
LOCATION 'Files/mydata'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # **Saving data in delta format**

# MARKDOWN ********************

# So far you've seen how to save a dataframe as a delta table (creating both the table schema definition in the metastore and the data files in delta format) and how to create the table definition (which creates the table schema in the metastore without saving any data files). A third possibility is to save data in delta format without creating a table definition in the metastore. This approach can be useful when you want to persist the results of data transformations performed in Spark in a file format over which you can later "overlay" a table definition or process directly by using the delta lake API.
# 
# For example, the following PySpark code saves a dataframe to a new folder location in delta format:

# CELL ********************

delta_path = "Files/mydatatable"
df.write.format("delta").save(delta_path)
# overwrite 
new_df.write.format("delta").mode("overwrite").save(delta_path)
new_rows_df.write.format("delta").mode("append").save(delta_path)
# add new rows

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # **Visualize data in Spark notebook**</mark>

# CELL ********************

from matplotlib import pyplot as plt
df_pd = df.toPandas()
plt.clf()
fig = plt.figure(figsize=(12,8))
plt.bar(x=df_pd['OrderYear'],height=df_pd['sum_quantity'],color='orange')
plt.title('Total Quantity by year')
plt.xlabel('Year')
plt.ylabel('Quantity')
plt.grid(color = '#95a5a6' , linestyle = '--' , linewidth = 2,axis ='y' , alpha = 0.7)
plt.xticks (rotation= 70)
plt.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************


# MARKDOWN ********************

# # **Optimization**

# MARKDOWN ********************

# # OptimizeWrite Function  
# Spark is a parallel-processing framework, with data stored on one or more worker nodes. In addition, Parquet files are immutable, with new files written for every update or delete. This process can result in Spark storing data in a large number of small files, known as the small file problem. It means that queries over large amounts of data can run slowly, or even fail to complete.  
# OptimizeWrite is a feature of Delta Lake which reduces the number of files as they're written. Instead of writing many small files, it writes fewer larger files. This helps to prevent the small files problem and ensure that performance isn't degraded.  
# In Microsoft Fabric, OptimizeWrite is enabled by default.

# CELL ********************

# Disable Optimize Write at the Spark session level
spark.conf.set("spark.microsoft.delta.optimizeWrite.enabled", False)

# Enable Optimize Write at the Spark session level
spark.conf.set("spark.microsoft.delta.optimizeWrite.enabled", True)

print(spark.conf.get("spark.microsoft.delta.optimizeWrite.enabled"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Optimize  
# A **table maintenance feature** that consolidates small Parquet files into fewer large files. You might run Optimize after loading large tables, resulting in:  
#     - fewer larger files  
#     - better compression  
#     - efficient data distribution across nodes  
# 
# **To run Optimize:**  
# 
#     - In Lakehouse Explorer, select the ... menu beside a table name and select Maintenance.
#     - Select Run OPTIMIZE command.
#     - Optionally, select Apply V-order to maximize reading speeds in Fabric.
#     - Select Run now.

# MARKDOWN ********************

# # V-Order function  
# When you run Optimize, you can optionally run V-Order, which is designed for the Parquet file format in Fabric. V-Order enables lightning-fast reads, with in-memory-like data access times. It also improves cost efficiency as it reduces network, disk, and CPU resources during reads.  
# V-Order is enabled by default in Microsoft Fabric and is applied as data is being written. It incurs a small overhead of about 15% making writes a little slower. However, V-Order enables faster reads from the Microsoft Fabric compute engines, such as Power BI, SQL, Spark, and others.  
# In Microsoft Fabric, the Power BI and SQL engines use Microsoft Verti-Scan technology which takes full advantage of V-Order optimization to speed up reads. Spark and other engines don't use VertiScan technology but still benefit from V-Order optimization by about 10% faster reads, sometimes up to 50%.  
# V-Order works by applying special sorting, row group distribution, dictionary encoding, and compression on Parquet files. It's 100% compliant to the open-source Parquet format and all Parquet engines can read it.
# V-Order might not be beneficial for write-intensive scenarios such as staging data stores where data is only read once or twice. In these situations, disabling V-Order might reduce the overall processing time for data ingestion.  
# Apply V-Order to individual tables by using the Table Maintenance feature by running the OPTIMIZE command.


# MARKDOWN ********************

# # Vacuum  
# Every time an update or delete is done, a new Parquet file is created and an entry is made in the transaction log. Old Parquet files are retained to enable time travel, which means that Parquet files accumulate over time.
# 
# The VACUUM command removes old Parquet data files, but not the transaction logs. When you run VACUUM, you can't time travel back earlier than the retention period.  
# The default retention period is 7 days (168 hours), and the system prevents you from using a shorter retention period. 
# 
# Run VACUUM on individual tables by using the Table maintenance feature:  
#     - In Lakehouse Explorer, select the ... menu beside a table name and select Maintenance.  
#     - Select Run VACUUM command using retention threshold and set the retention threshold.  
#     - Select Run now.  
# 
# You can also run VACUUM as a SQL command in a notebook:    
# %%sql  
# VACUUM lakehouse2.products RETAIN 168 HOURS;  
# 
# VACUUM commits to the Delta transaction log, so you can view previous runs in DESCRIBE HISTORY.  
# %%sql  
# DESCRIBE HISTORY lakehouse2.products;


# MARKDOWN ********************

# # Partitioning Delta tables  
# Delta Lake allows you to organize data into partitions. This might improve performance by enabling data skipping, which boosts performance by skipping over irrelevant data objects based on an object's metadata.
# Consider a situation where large amounts of sales data are being stored. You could partition sales data by year. The partitions are stored in subfolders named "year=2021", "year=2022", etc. If you only want to report on sales data for 2024, then the partitions for other years can be skipped, which improves read performance.
# Partitioning of small amounts of data can degrade performance, however, because it increases the number of files and can exacerbate the "small files problem."
# 
# Use partitioning when:
# - You have very large amounts of data.
# - Tables can be split into a few large partitions.
# 
# Don't use partitioning when:
# - Data volumes are small.
# - A partitioning column has high cardinality, as this creates a large number of partitions.
# - A partitioning column would result in multiple levels.


# MARKDOWN ********************

# # Time travel - Table versioning

# CELL ********************

# View the history
%%sql
DESCRIBE HISTORY sales_data 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Retrieve data from a specific version
 
# df_retrieved = spark.read.format("delta").option("versionAsOf",0).load(delta_path)
df_retrieved = spark.read.format("delta").option("versionAsOf",0).table("sales_data")
df_retrieved = df_retrieved.where(col("CustomerName") == "Kelvin Huang")
display(df_retrieved)
#df_filtered_by_col_row  = df_with_year["CustomerName","EmailAddress"].where(col("CustomerName").like("%Jan%"))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Retrieve data by specifying a timestamp
df_retrieved = spark.read.format("delta").option("timestampAsOf",'2022-01-01').table("sales_data")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
