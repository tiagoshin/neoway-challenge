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

# In[2]:


from pyspark.sql.window import Window
import pyspark.sql.functions as f


# ### Reading data
# For this process we just need the columns Names, Churn and Onboard_date

# In[3]:


df = spark.read.parquet(f"gs://{DATA_BUCKET}/raw/churned_customers").select("Names", "Onboard_date")


# ### Appling transformations
# For this case we're assuming that the data is consistent in a way that every

# In[4]:


df = df.withColumn("rank", f.rank().over(Window().partitionBy("Names").orderBy(f.asc("Onboard_Date")))).withColumn("Churned_Before", f.when(f.col("rank") > 1, f.lit(1)).otherwise(f.lit(0))).drop("rank", "Onboard_date")


# ### Write data to BigQuery

# In[5]:


spark.conf.set('temporaryGcsBucket', BIGQUERY_BUCKET)


# In[7]:


df.write.partitionBy("Names").format('bigquery') .mode('overwrite').option('table', 'feature_store.customers_churned_before') .save()


# In[ ]:




