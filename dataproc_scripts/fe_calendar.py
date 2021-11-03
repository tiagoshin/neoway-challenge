#!/usr/bin/env python
# coding: utf-8

# In[1]:


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

# In[3]:


import pandas as pd
from datetime import datetime
from pyspark.sql.types import StructType, StructField, DateType
import pyspark.sql.functions as f


# ### Creating dataframes with dates
# The goal here is to have all the possible dates in order to get pre computed features

# In[3]:


schema = StructType([StructField("date", DateType(), True)])


# In[4]:


df = spark.createDataFrame(pd.DataFrame(                              pd.date_range(start="2000-01-01", end="2050-01-01", freq="D", name="Date")), 
                 schema=schema)


# ### Applying feature engineering

# In[5]:


df = df.withColumn("Onboard_Year", f.year("Date").cast("double")).withColumn("Onboard_Month", f.month("Date").cast("double")).withColumn("Onboard_DayOfMonth", f.dayofmonth("Date").cast("double")).withColumn("Onboard_DayOfWeek", f.dayofweek("Date").cast("double")).withColumn("Onboard_Quarter", f.quarter("Date").cast("double")).withColumn("Onboard_WeekOfYear", f.weekofyear("Date").cast("double")).withColumn("Date_index", f.date_format(f.col("Date"), "yyyyMMdd"))


# ### Write data to BigQuery

# In[6]:


spark.conf.set('temporaryGcsBucket', BIGQUERY_BUCKET)


# In[7]:


df.write.partitionBy("Date_index").format('bigquery') .mode('overwrite').option('table', 'feature_store.calendar_features') .save()

