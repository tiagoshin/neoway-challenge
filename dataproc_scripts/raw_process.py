#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

sc = SparkContext()
spark = SparkSession(sc)

import os, tempfile
tmp = tempfile.NamedTemporaryFile(delete=False)


# In[ ]:


DATA_BUCKET="neoway-challenge"
BIGQUERY_BUCKET="bigquery-tempbucket-nc"
SCRIPTS_BUCKET="dataproc_shin_scripts"


# ### Importing libraries

# In[8]:


from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DoubleType, TimestampType
import pyspark.sql.functions as f


# ### Reading data
# Enforce schema to write with the right schema in parquet format

# In[9]:


schema=StructType([
    StructField("Names",StringType(), True),
    StructField("Age", DoubleType(), True),
    StructField("Total_Purchase",DoubleType(), True),
    StructField("Account_Manager",IntegerType(), True),
    StructField("Years",DoubleType(), True),
    StructField("Num_Sites",DoubleType(), True),
    StructField("Onboard_date",TimestampType(), True),
    StructField("Location",StringType(), True),
    StructField("Company",StringType(), True),
    StructField("Churn",IntegerType(), True)
])


# In[11]:


df = spark.read.csv(f"gs://{DATA_BUCKET}/lnd/churned_customers.csv", header=True, schema=schema)


# ### Write
# As we don't have any specific requirements, just overwrite the data

# In[12]:


df.write.format("parquet").mode("overwrite").save(f"gs://{DATA_BUCKET}/raw/churned_customers")

