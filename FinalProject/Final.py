# Databricks notebook source
# MAGIC %md
# MAGIC ### Import necessary libraries

# COMMAND ----------

from zipfile import ZipFile
import os
import json
import pymongo
import pyspark.pandas as pd
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, BinaryType
from pyspark.sql.types import ByteType, ShortType, IntegerType, LongType, DecimalType

import jaydebeapi as jdbc
import sys


# COMMAND ----------

# MAGIC %md
# MAGIC ### Connect to Kaggle API
# MAGIC Source for all Kaggle related work: https://github.com/MrFuguDataScience/various_API_connections/blob/master/Kaggle_api_basics.ipynb

# COMMAND ----------

api_token = {"username":"kqn3ryn","key":"4824461b985d3c075664cc2ebf9fb922"}

import json

with open('/root/.kaggle/kaggle.json', 'w') as file:
    json.dump(api_token, file)

!chmod 600 ~/.kaggle/kaggle.json

# COMMAND ----------

#connect to Kaggle API
from kaggle.api.kaggle_api_extended import KaggleApi
api = KaggleApi()
api.authenticate()

# COMMAND ----------

# MAGIC %md
# MAGIC Find dataset from Kaggle using the API

# COMMAND ----------

api.dataset_list_files('vivek468/superstore-dataset-final').files

# COMMAND ----------

# MAGIC %md
# MAGIC Make a directory to save zip file to

# COMMAND ----------

# import os

# define the name of the directory to be created
path = os.getcwd()+"/DS3002-final"

try:
    os.mkdir(path)
except OSError:
    print ("Creation of the directory %s failed" % path)
else:
    print ("Successfully created the directory %s " % path)

# COMMAND ----------

# MAGIC %md
# MAGIC Save file to path

# COMMAND ----------

api.dataset_download_files('vivek468/superstore-dataset-final', '/databricks/driver/DS3002-final')

# COMMAND ----------

# MAGIC %md
# MAGIC List what is in the file and save it as an array

# COMMAND ----------

superstore=!ls /databricks/driver/DS3002-final
superstore

# COMMAND ----------

# MAGIC %md
# MAGIC Function to find path of file

# COMMAND ----------

def os_dir_search(file):
    u=[]
    for p,n,f in os.walk(os.getcwd()):
        
        for a in f:
            a = str(a)
            if a.endswith(file): # can be (.csv) or a file like I did and search 
#                 print(a)
#                 print(p)
                t=p
    return t

os_dir_search(superstore[0])

# COMMAND ----------

# MAGIC %md
# MAGIC Unzip file to csv and save to path

# COMMAND ----------

# from zipfile import ZipFile
# specifying the zip file name 

file_name=os_dir_search(superstore[0])+'/'+superstore[0]
  
# opening the zip file in READ mode 
with ZipFile(file_name, 'r') as zip: 
    # printing all the contents of the zip file 
    zip.printdir() 
  
    # extracting all the files 
    print('Extracting all the files now...') 
    zip.extractall('/databricks/driver/DS3002-final') 
    print('Done!')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read in csv file into dataframe

# COMMAND ----------

dbutils.fs.cp("file:/databricks/driver/DS3002-final/Sample - Superstore.csv", 
   "/FileStore/Superstore.csv")
df = spark.read.csv("/FileStore/Superstore.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC Edit dataframe to have correct column titles and delete first row.
# MAGIC <br/><br/>
# MAGIC ***Note: needed to make these changes becasue of the way the file was read in

# COMMAND ----------

# Source: https://sparkbyexamples.com/pyspark/pyspark-rename-dataframe-column/
# Ranaming columns and dropping row becasue of thw way the csv was read from kaggle API
df = df.withColumnRenamed("_c0","RowID") \
    .withColumnRenamed("_c1","OrderID") \
    .withColumnRenamed("_c2","OrderDate") \
    .withColumnRenamed("_c3","ShipDate") \
    .withColumnRenamed("_c4","ShipMode") \
    .withColumnRenamed("_c5","CustomerID") \
    .withColumnRenamed("_c6","CustomerName") \
    .withColumnRenamed("_c7","Segment") \
    .withColumnRenamed("_c8","Country") \
    .withColumnRenamed("_c9","City") \
    .withColumnRenamed("_c10","State") \
    .withColumnRenamed("_c11","PostalCode") \
    .withColumnRenamed("_c12","Region") \
    .withColumnRenamed("_c13","ProductID") \
    .withColumnRenamed("_c14","Category") \
    .withColumnRenamed("_c15","SubCategory") \
    .withColumnRenamed("_c16","ProductName") \
    .withColumnRenamed("_c17","Sales") \
    .withColumnRenamed("_c18","Quantity") \
    .withColumnRenamed("_c19","Discount") \
    .withColumnRenamed("_c20","Profit") \

df = df.filter(df.RowID!='Row ID')
df.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Connect to Azure SQL server

# COMMAND ----------

jdbcHostname = "kqn3ryn-sqlsvr.database.windows.net"
jdbcDatabase = "superstore"
jdbcPort = 1433
jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname, jdbcPort, jdbcDatabase) 

connectionProperties = {
  "user" : "kqn3ryn",
  "password" : "Quynhanh01",
  "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# COMMAND ----------

# MAGIC %md
# MAGIC Populate 'superstore' data base with superstore_data table using csv (database was already created in Azure)

# COMMAND ----------

# Source: https://community.databricks.com/s/question/0D53f00001gsZ3QCAU/cant-write-big-dataframe-into-mssql-server-by-using-jdbc-driver-on-azure-databricks
# Populating table in superstore database using the csv
username = 'kqn3ryn'
password = 'Quynhanh01'
tablename = 'superstore_data'
batch_size = 9995

df.write \
            .format("jdbc") \
            .mode("overwrite") \
            .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
            .option("url", jdbcUrl) \
            .option("dbtable", tablename) \
            .option("user", username) \
            .option("password", password) \
            .option("batchsize", batch_size) \
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC Read in new table from SQL to show it has been properly populated.

# COMMAND ----------

sql_query = """
(SELECT * FROM [dbo].[superstore_data]) superstore
"""

superstore_data = spark.read.jdbc(url=jdbcUrl, table=sql_query, properties=connectionProperties)
display(superstore_data)

# COMMAND ----------

# MAGIC %md
# MAGIC I have created the following dimension tables in Azure Data Studio: dim_customers, dim_products, and dim_orders. I then read each table into a data frame below and saved it as a table in the Data Bricks File System (DBFS).

# COMMAND ----------

sql_query = """
(SELECT * FROM [dbo].[dim_customers]) superstore
"""

dim_customers = spark.read.jdbc(url=jdbcUrl, table=sql_query, properties=connectionProperties)
display(dim_customers)

# COMMAND ----------

dim_customers.write.mode("overwrite").saveAsTable("dim_customers")

# COMMAND ----------

sql_query = """
(SELECT * FROM [dbo].[dim_products]) superstore
"""

dim_products = spark.read.jdbc(url=jdbcUrl, table=sql_query, properties=connectionProperties)
display(dim_products)

# COMMAND ----------

dim_products.write.mode("overwrite").saveAsTable("dim_products")

# COMMAND ----------

sql_query = """
(SELECT * FROM [dbo].[dim_orders]) superstore
"""

dim_orders = spark.read.jdbc(url=jdbcUrl, table=sql_query, properties=connectionProperties)
display(dim_orders)

# COMMAND ----------

dim_orders.write.mode("overwrite").saveAsTable("dim_orders")

# COMMAND ----------

# MAGIC %md
# MAGIC Using these dimension tables, I have created a fact table below using the dimension tables saved in the DBFS. 
# MAGIC <br><br>
# MAGIC I Have also provided code to create the fact table in azure. 
# MAGIC <br>
# MAGIC ***Note: Execution was unsuccessful due to azure free trial restrictions, however, the code is correct

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT c.customerKey,
# MAGIC 	c.customerName,
# MAGIC     c.customerType,
# MAGIC     c.country,
# MAGIC     c.city,
# MAGIC     c.state,
# MAGIC     c.zipCode,
# MAGIC     c.region,
# MAGIC     o.orderKey,
# MAGIC     o.orderDate,
# MAGIC     o.shipDate,
# MAGIC     o.shipMode,
# MAGIC     p.productKey,
# MAGIC     p.productCategory,
# MAGIC     p.productSubCategory,
# MAGIC     p.productName,
# MAGIC     p.price,
# MAGIC     p.quantity,
# MAGIC     p.discount,
# MAGIC     o.profit
# MAGIC FROM dim_customers AS c
# MAGIC INNER JOIN dim_orders AS o
# MAGIC ON c.orderID = o.orderKey
# MAGIC RIGHT OUTER JOIN dim_products AS p
# MAGIC ON o.productID = p.productKey

# COMMAND ----------

sql_query = """
(SELECT c.customerKey, 
    c.customerName,
    c.customerType,
    c.country,
    c.city, 
    c.state,
    c.zipCode,
    c.region,
    o.orderKey,
    o.orderDate,
    o.shipDate,
    o.shipMode,
    p.productKey,
    p.productCategory,
    p.productSubCategory,
    p.productName,
    p.price,
    p.quantity,
    p.discount,
    o.profit
FROM dbo.dim_customers AS c
INNER JOIN dbo.dim_orders AS o
ON c.orderID = o.orderKey
INNER JOIN dbo.dim_products AS p
ON o.productID = p.productKey) superstore
"""

fact_superstore = spark.read.jdbc(url=jdbcUrl, table=sql_query, properties=connectionProperties)
display(fact_superstore)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Connect to MongoDB Atlas

# COMMAND ----------

atlas_cluster_name = "ds3002"
atlas_default_dbname = "superstore"
atlas_user_name = "kqn3ryn"
atlas_password = "Quynhanh01"

conn_str = f"mongodb+srv://{atlas_user_name}:{atlas_password}@ds3002.mii28.mongodb.net/{atlas_default_dbname}?retryWrites=true&w=majority"

client = pymongo.MongoClient(conn_str)
client.list_database_names()

# COMMAND ----------

db_name = "superstore"

db = client[db_name]
db.list_collection_names()

# COMMAND ----------

collection = "superstore_data"

superstore_data = db[collection]
superstore_data.find_one()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load collections onto MongoDB from json files that I uploaded in the DBFS

# COMMAND ----------

# ######################################################################################################################
# Use this Function to Create New Collections by Uploading JSON file(s) to the MongoDB Atlas server.
# ######################################################################################################################
def set_mongo_collection(user_id, pwd, cluster_name, db_name, src_file_path, json_files):
    '''Create a client connection to MongoDB'''
    mongo_uri = f"mongodb+srv://{user_id}:{pwd}@{cluster_name}.mii28.mongodb.net/{db_name}?retryWrites=true&w=majority"
    client = pymongo.MongoClient(mongo_uri)
    db = client[db_name]
    
    '''Read in a JSON file, and Use It to Create a New Collection'''
    for file in json_files:
        db.drop_collection(file)
        json_file = os.path.join(src_file_path, json_files[file])
        with open(json_file, 'r') as openfile:
            json_object = json.load(openfile)
            file = db[file]
            result = file.insert_many(json_object)

    client.close()
    
    return result

# COMMAND ----------

# MAGIC %md 
# MAGIC ***Note: this was unsuccessful becasue I was receiving this error "FileNotFoundError: [Errno 2] No such file or directory: '/dbfs/FileStore/tables/dim_customers.json'"" for all files: dim_customers.json, dim_orders.json, and dim_products.json

# COMMAND ----------

src_dbname = "superstore"
src_dir = '/dbfs/FileStore/tables'
json_files = {"dim_customers" : "dim_customers.json"}

set_mongo_collection(atlas_user_name, atlas_password, atlas_cluster_name, src_dbname, src_dir, json_files)

# COMMAND ----------

json_files = {"dim_customers" : "dim_orders.json"}

set_mongo_collection(atlas_user_name, atlas_password, atlas_cluster_name, src_dbname, src_dir, json_files)

# COMMAND ----------

json_files = {"dim_customers" : "dim_products.json"}

set_mongo_collection(atlas_user_name, atlas_password, atlas_cluster_name, src_dbname, src_dir, json_files)

# COMMAND ----------

# MAGIC %md
# MAGIC However, the files do exist and I have provided proof below.

# COMMAND ----------

# MAGIC %fs
# MAGIC ls FileStore/tables
