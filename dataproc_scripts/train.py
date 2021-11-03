#!/usr/bin/env python
# coding: utf-8

# In[6]:


from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

sc = SparkContext()
spark = SparkSession(sc)

import os, tempfile
tmp = tempfile.NamedTemporaryFile(delete=False)


# In[7]:


DATA_BUCKET="neoway-challenge"
BIGQUERY_BUCKET="bigquery-tempbucket-nc"
SCRIPTS_BUCKET="dataproc_shin_scripts"


# ### Import libraries

# In[2]:


import pyspark.sql.functions as f
import json
from datetime import datetime
from google.cloud import storage
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler, StandardScaler, OneHotEncoder
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.functions import vector_to_array
from pyspark.mllib.evaluation import BinaryClassificationMetrics, MulticlassMetrics
from onnxmltools import convert_sparkml
from onnxmltools.convert.sparkml.utils import buildInitialTypesSimple


# #### Setup bigquery bucket

# In[3]:


spark.conf.set('temporaryGcsBucket', BIGQUERY_BUCKET)


# #### Changing default spark conf values

# In[4]:


spark.conf.set("spark.sql.debug.maxToStringFields", 1000)


# ### Read data from data lake

# In[5]:


df = spark.read.parquet(f"gs://{DATA_BUCKET}/raw/churned_customers")


# ### Read from feature stores

# In[6]:


customer_features = spark.read.format('bigquery').option('table', 'feature_store.customers_churned_before').load()


# In[7]:


calendar_features = spark.read.format('bigquery').option('table', 'feature_store.calendar_features').load()


# ### Joining the features

# In[8]:


df = df.withColumn("Date_index", f.date_format(f.col("Onboard_Date"), "yyyyMMdd"))


# In[9]:


df = df.join(customer_features, ["Names"], "inner").join(calendar_features, ['Date_index'], "inner")


# ### Enforce data types

# In[10]:


cast_dict = [{"col_name": "Age", "type": "double"},
             {"col_name": "Total_Purchase", "type": "double"},
             {"col_name": "Account_Manager", "type": "string"},
             {"col_name": "Years", "type": "double"},
             {"col_name": "Num_Sites", "type": "double"},
             {"col_name": "Location", "type": "string"},
             {"col_name": "Company", "type": "string"},
             {"col_name": "Onboard_Year", "type": "double"},
             {"col_name": "Onboard_Month", "type": "double"},
             {"col_name": "Onboard_DayOfMonth", "type": "double"},
             {"col_name": "Onboard_DayOfWeek", "type": "double"},
             {"col_name": "Onboard_Quarter", "type": "double"},
             {"col_name": "Onboard_WeekOfYear", "type": "double"},
             {"col_name": "Churned_Before", "type": "string"},
             {"col_name": "Churn", "type": "double"},
            ]

for c in cast_dict:
    df = df.withColumn(c["col_name"], f.col(c["col_name"]).cast(c["type"]))


# ### Select useful columns

# In[11]:


df = df.select(*[i['col_name'] for i in cast_dict])


# ### Define variables and target

# In[12]:


target = "Churn"
categorical_variables = list(set([i['col_name'] for i in cast_dict if i['type'] == 'string']) - set([target]))
numerical_variables = list(set([i['col_name'] for i in cast_dict if i['type'] == 'double']) - set([target]))


# ### Create weight column
# It will be used to deal with imbalanced labels

# In[13]:


df = df.withColumn("weight", f.when(f.col(target) == 1, f.lit(5)).otherwise(f.lit(1)))


# ### Creating preprocessing pipeline

# In[14]:


indexers = [StringIndexer(inputCol=col, outputCol=col+"_indexed", handleInvalid="keep") for col in categorical_variables]
onehotencoders = [OneHotEncoder(inputCols=[col+"_indexed"], outputCols=[col+"_encoded"], dropLast=False, handleInvalid="keep") for col in categorical_variables]
vectorassemblers = [VectorAssembler(inputCols=[col], outputCol=col+"_vectorized") for col in numerical_variables]
standardscalers = [StandardScaler(inputCol=col+"_vectorized", outputCol=col+"_standarized", withMean=False, withStd=True) for col in numerical_variables]
finalassembler = VectorAssembler(inputCols=list(set([var + "_encoded" for var in categorical_variables]) | set([var + "_standarized" for var in numerical_variables])), outputCol="features")


# ### Get params from the best rf model and create the random forest model

# In[15]:


storage_client = storage.Client()
bucket = storage_client.bucket(DATA_BUCKET)
list_of_runs = sorted([int(b.name[4:18]) for b in bucket.list_blobs() if b.name[0:3] == "run"], reverse=True)
last_run = list_of_runs[0]
blob = bucket.blob(f'run/{last_run}/rf_best_params.json')
rf_best_params = json.loads(blob.download_as_string())


# In[16]:


print(rf_best_params)


# In[17]:


rf = RandomForestClassifier(labelCol=target, featuresCol="features", weightCol="weight", 
                            maxBins=rf_best_params["maxBins"], 
                            maxDepth=rf_best_params["maxDepth"],
                            numTrees=rf_best_params["numTrees"],
                            seed=42)


# In[18]:


stages = indexers
stages.extend(onehotencoders + vectorassemblers + standardscalers)
stages.extend([finalassembler, rf])


# ### Fitting training data to preprocessing pipeline

# In[19]:


pipeline = Pipeline(stages=stages).fit(df)


# ### Saving pipeline for inference using onnx

# In[20]:


initial_types = buildInitialTypesSimple(df.select(categorical_variables+numerical_variables).limit(1))


# In[22]:


onnx_model = convert_sparkml(pipeline, 'Pyspark pipeline model', initial_types, spark_session = spark)


# In[23]:


blob = bucket.blob('model/onnx_pipeline.onnx')
blob.upload_from_string(onnx_model.SerializeToString())


# In[ ]:




