#!/usr/bin/env python
# coding: utf-8

# In[6]:


class FilesPath(object): 
    customer_file = "/home/unicorp/evolve/data/starter/customers.csv"  #change file path of customer.csv
    product_file = "/home/unicorp/evolve/data/starter/products.csv"     #change file path of product.csv
    transactionFiles = "/home/unicorp/evolve/data/starter/transactions/"  #change directory till transactions
    outWriteFilePath = ""


from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import flatten
from pyspark.sql.functions import udf
import datetime, time 
from pyspark.sql import Window
from  pyspark.sql.types import TimestampType
from pyspark.sql.functions import to_date,lit,count



spark = SparkSession     .builder     .appName("EVOLVE")     .getOrCreate()

def _to_timestamp(s):
    return datetime.datetime.strptime(s, '%Y-%m-%d %H:%M:%S.%f')

udf_to_timestamp = udf(_to_timestamp, TimestampType())

def createDfCsv(s):
    return spark.read.options(header='True', inferSchema='True').csv(s)

def createDfJson(s):
    return spark.read.options(header='True', inferSchema='True').json(s)

df1 = createDfCsv(FilesPath.customer_file)

df2 = createDfCsv(FilesPath.product_file)

df4 = createDfJson(FilesPath.transactionFiles +"*/*.json")
    

def _to_timestamp(s):
    return datetime.datetime.strptime(s, '%Y-%m-%d %H:%M:%S.%f')

udf_to_timestamp = udf(_to_timestamp, TimestampType())
w = Window.partitionBy('product_id')

df5 = df4.select('date_of_purchase','basket','customer_id').withColumn("date_of_purchase", udf_to_timestamp("date_of_purchase"))


df6= df5.select(df5.date_of_purchase ,df5.customer_id,explode(df5.basket.product_id).alias("product_id"))



FinalWriteDF = df1.join(df6, df1.customer_id == df6.customer_id).join(df2, df2.product_id==df6.product_id).select(lit(df1["loyalty_score"]),df6["*"],df2['product_category']).select('customer_id', lit('loyalty_score'), 'date_of_purchase', 'product_category','product_id' ,count('product_id').over(w).alias('purchase_count')).sort('customer_id').drop('date_of_purchase').dropDuplicates()#.show(truncate=False)
FinalWriteDF.printSchema()



FinalWriteDF.show(600)
FinalWriteDF.write.csv('finalDataCsvWithoutTimeFrame.csv')


# In[ ]:




