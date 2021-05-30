#!/usr/bin/env python
# coding: utf-8

# In[ ]:





# In[5]:


import unittest
import pyspark
from pyspark.sql import SparkSession
from unittest_pyspark import as_list, get_spark
import pyspark.sql.types as pst

class PySparkTestCase(unittest.TestCase):
    

    @classmethod
    def setUpClass(self):
        self.spark= SparkSession.builder         .master("local")        .appName("revolve")        .getOrCreate()
    def tearDown(self):
        """
        Stop Spark
        """
        self.spark.stop()


class Test_Spark(PySparkTestCase):
    

    def test(self):
        input = [ pst.Row(a=1, b=2)]
        input_df = self.spark.createDataFrame(input)

        expect = [{'a':1}]

        actual_df = input_df.select("a")
        actual = as_list(actual_df)

        self.assertEqual(actual, expect)


class CheckNone(PySparkTestCase):
    

    def test_with_df(self):
        df=  self.spark.read.options(header='True', inferSchema='True').csv("/home/unicorp/evolve/data/starter/customers.csv")
                                                                       #put path of customer csv file
        df.createOrReplaceTempView("customer")
        df1=  self.spark.read.options(header='True', inferSchema='True').csv("/home/unicorp/evolve/data/starter/products.csv")
                                                                       #put path of customer csv file
        df1.createOrReplaceTempView("product")
        null_for_customer= self.spark.sql("SELECT * FROM customer where customer_id IS NULL or loyalty_score IS NULL")
        null_for_product= self.spark.sql("SELECT * FROM product where product_id  IS NULL or product_description IS NULL or product_category IS NULL")
        message ="None value present"
        self.assertEqual(null_for_customer.count(), 0,message)
        self.assertEqual(null_for_product.count(), 0,message)


class TestForCustomerCsv(PySparkTestCase):
   

    def test_with_df(self):
        df=  self.spark.read.options(header='True', inferSchema='True').csv("/home/unicorp/evolve/data/starter/customers.csv")
                                                                       #put path of customer csv file
        message = "First value and second value are not equal !"
        self.assertEqual(df.count(), 137,message)

class TestForProductCsv(PySparkTestCase):
   

    def test_with_df(self):
        df=  self.spark.read.options(header='True', inferSchema='True').csv("/home/unicorp/evolve/data/starter/products.csv")
                                                                        #put path of product csv file
        message = "First value and second value are not equal !"
        self.assertEqual(df.count(), 64,message)
        

        
class TestForTransactionJson(PySparkTestCase):
   

    def test_with_df(self):
        df=  self.spark.read.options(header='True', inferSchema='True').json("/home/unicorp/evolve/data/starter/transactions/*/*.json") 
                                                                        #put path of transaction json folder
        
        message = "First value and second value are not equal !"
        self.assertEqual(df.count(), 2229,message)
        
class TestForSparkRDD(PySparkTestCase):       
    


    def test_for_rdd(self):
        test_input = [
            ' hello spark ',
            ' hello again spark spark'
        ]

        input_rdd = self.spark.sparkContext.parallelize(test_input, 1)

        from operator import add

        results = input_rdd.flatMap(lambda x: x.split()).map(lambda x: (x, 1)).reduceByKey(add).collect()
        self.assertEqual(results, [('hello', 2), ('spark', 3), ('again', 1)])
        print("test done")
  
    
unittest.main(argv=[''],verbosity=2, exit=False)


# In[ ]:





# In[ ]:




