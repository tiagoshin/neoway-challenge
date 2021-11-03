#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

sc = SparkContext()
spark = SparkSession(sc)

import os, tempfile
tmp = tempfile.NamedTemporaryFile(delete=False)


# In[1]:


DATA_BUCKET="neoway-challenge"
BIGQUERY_BUCKET="bigquery-tempbucket-nc"
SCRIPTS_BUCKET="dataproc_shin_scripts"


# ### Import libraries

# In[58]:


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


# ### Define run id for saving artifacts

# In[59]:


run_id = datetime.strftime(datetime.now(), "%Y%m%d%H%M%S")


# #### Setup bigquery bucket

# In[60]:


spark.conf.set('temporaryGcsBucket', BIGQUERY_BUCKET)


# #### Changing default spark conf values

# In[61]:


spark.conf.set("spark.sql.debug.maxToStringFields", 1000)


# ### Read data from data lake

# In[62]:


df = spark.read.parquet(f"gs://{DATA_BUCKET}/raw/churned_customers")


# ### Read from feature stores

# In[63]:


customer_features = spark.read.format('bigquery').option('table', 'feature_store.customers_churned_before').load()


# In[64]:


calendar_features = spark.read.format('bigquery').option('table', 'feature_store.calendar_features').load()


# ### Joining the features

# In[65]:


df = df.withColumn("Date_index", f.date_format(f.col("Onboard_Date"), "yyyyMMdd"))


# In[66]:


df = df.join(customer_features, ["Names"], "inner").join(calendar_features, ['Date_index'], "inner")


# ### Enforce data types

# In[67]:


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


# ### Define variables and target

# In[68]:


target = "Churn"
categorical_variables = list(set([i['col_name'] for i in cast_dict if i['type'] == 'string']) - set([target]))
numerical_variables = list(set([i['col_name'] for i in cast_dict if i['type'] == 'double']) - set([target]))


# ### Create weight column
# It will be used to deal with imbalanced labels

# In[69]:


df = df.withColumn("weight", f.when(f.col(target) == 1, f.lit(5)).otherwise(f.lit(1)))


# ### Train test split

# In[86]:


train, test = df.randomSplit([0.7, 0.3], seed=42)


# ### Select useful columns in train dataset

# In[87]:


train = train.select(*[i['col_name'] for i in cast_dict] + [target, "weight"])


# ### Creating preprocessing pipeline

# In[88]:


indexers = [StringIndexer(inputCol=col, outputCol=col+"_indexed", handleInvalid="keep") for col in categorical_variables]
onehotencoders = [OneHotEncoder(inputCols=[col+"_indexed"], outputCols=[col+"_encoded"], dropLast=False, handleInvalid="keep") for col in categorical_variables]
vectorassemblers = [VectorAssembler(inputCols=[col], outputCol=col+"_vectorized") for col in numerical_variables]
standardscalers = [StandardScaler(inputCol=col+"_vectorized", outputCol=col+"_standarized", withMean=False, withStd=True) for col in numerical_variables]
finalassembler = VectorAssembler(inputCols=list(set([var + "_encoded" for var in categorical_variables]) | set([var + "_standarized" for var in numerical_variables])), outputCol="features")


# In[89]:


stages = indexers
stages.extend(onehotencoders + vectorassemblers + standardscalers)
stages.extend([finalassembler])


# ### Fitting training data to preprocessing pipeline

# In[90]:


pipeline = Pipeline(stages=stages).fit(train)


# ### Transforming data according to the pipeline and select only useful columns
# Note that all the variables are now represented as a "features" vector

# In[91]:


train = pipeline.transform(train).select("features", target, "weight")
test = pipeline.transform(test)


# ### Create model with hyperparameter search and cross validation

# In[92]:


rf = RandomForestClassifier(labelCol=target, featuresCol="features", weightCol="weight", seed=42)


# In[93]:


evaluator = BinaryClassificationEvaluator(rawPredictionCol="prediction", labelCol=target, metricName="areaUnderROC")


# In[94]:


grid = ParamGridBuilder()   .addGrid(rf.maxDepth, [6, 16])   .addGrid(rf.maxBins, [5])   .addGrid(rf.numTrees, [12])   .build()


# In[95]:


cv = CrossValidator(estimator=rf, evaluator=evaluator, estimatorParamMaps=grid, numFolds=3)


# In[96]:


cvModel = cv.fit(train)


# ### Get and Save the params from the best model

# In[97]:


rf_best_params = {"maxBins": cvModel.bestModel._java_obj.getMaxBins(),
              "maxDepth": cvModel.bestModel._java_obj.getMaxDepth(), 
              "numTrees": cvModel.bestModel._java_obj.getNumTrees()}


# In[98]:


storage_client = storage.Client()
bucket = storage_client.bucket(DATA_BUCKET)
blob = bucket.blob(f'run/{run_id}/rf_best_params.json')
blob.upload_from_string(data=json.dumps(rf_best_params), content_type='application/json')


# ### Save prediction object for post analysis

# In[133]:


prediction = cvModel.transform(test)


# In[134]:


prediction.write.format('parquet').mode("overwrite").save(f"gs://{DATA_BUCKET}/run/{run_id}/test_predictions")


# ### Get and save metrics from the best model using the test dataset

# In[102]:


prediction_to_evaluate = prediction.withColumn("label", f.col(target).cast("double")).select("prediction", "label")
metrics = MulticlassMetrics(prediction_to_evaluate.rdd.map(tuple))


# In[173]:


confusion_matrix = metrics.confusionMatrix().toArray()
tp = confusion_matrix[0][0]
fn = confusion_matrix[1][0]
fp = confusion_matrix[0][1]
tn = confusion_matrix[1][1]


# In[174]:


rf_metrics = {"AreaUnderROC": BinaryClassificationEvaluator(labelCol=target, metricName="areaUnderROC", weightCol="weight").evaluate(prediction),
"AreaUnderPR": BinaryClassificationEvaluator(labelCol=target, metricName="areaUnderPR", weightCol="weight").evaluate(prediction),
"Precision": tp/(tp+fp),
"Recall": tp/(tp+fn),
"FMeasure": (2*tp)/((2*tp)+fp+fn),
"FalsePositiveRate":fp/(fn+tn),
"ConfusionMatrix": confusion_matrix.tolist()
}


# In[180]:


blob = bucket.blob(f'run/{run_id}/rf_metrics.json')
blob.upload_from_string(data=json.dumps(rf_metrics), content_type='application/json')


# In[ ]:




