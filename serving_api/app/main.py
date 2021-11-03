import os
from fastapi import FastAPI, Depends, HTTPException
from pydantic import BaseModel
from datetime import datetime
import json
import numpy as np

from sqlalchemy import text
from sqlalchemy.engine import create_engine
import onnxruntime
from google.cloud import storage

app = FastAPI()

class Customer(BaseModel):
    Names: str
    Age: float
    Total_Purchase: float
    Account_Manager: int
    Years: float
    Num_Sites: float
    Onboard_date: str
    Location: str
    Company: str

@app.get('/')
async def root():
    return {'message': 'Hello World!'}

@app.put("/evaluate")
def evaluate(customer: Customer):
    features = _get_features(json.loads(customer.json()))
    decision = _get_decision(features)
    return {'Churn': decision}

def _get_features(customer): 
    query_customer = f"SELECT Churned_Before FROM `feature_store.customers_churned_before` WHERE Names = '{customer['Names']}' limit 1"
    try:
        customer["Date_index"] = datetime.strftime(datetime.strptime(customer["Onboard_date"], '%Y-%m-%dT%H:%M:%S.%fZ'), '%Y%m%d')
    except ValueError:
        raise HTTPException(status_code=404, detail="Date in the wrong format. It should be exactly '%Y-%m-%dT%H:%M:%S.%fZ', example: '2014-09-19T08:46:46.000Z'")        
    query_calendar = f"SELECT Onboard_Year, Onboard_Month, Onboard_DayOfMonth, Onboard_DayOfWeek, Onboard_Quarter, Onboard_WeekOfYear FROM `feature_store.calendar_features` WHERE Date_index = '{customer['Date_index']}' limit 1"
    engine = create_engine(f"bigquery://{os.environ['GOOGLE_PROJECT']}")
    with engine.connect() as connection:
        customer_features = connection.execute(text(query_customer)).first()
        calendar_features = connection.execute(text(query_calendar)).first()
    if customer_features is not None:
        customer["Churned_Before"] = customer_features[0]
    else:
        customer["Churned_Before"] = 0
    if calendar_features is not None:
        customer["Onboard_Year"] = calendar_features[0]
        customer["Onboard_Month"] = calendar_features[1]
        customer["Onboard_DayOfMonth"] = calendar_features[2]
        customer["Onboard_DayOfWeek"] = calendar_features[3]
        customer["Onboard_Quarter"] = calendar_features[4]
        customer["Onboard_WeekOfYear"] = calendar_features[5]
    else:
        raise HTTPException(status_code=404, detail="Invalid date. Please check the format and select a date between year 2000 and 2050.")        

    for key in ["Names", "Date_index", "Onboard_date"]:
        customer.pop(key)    
    return customer

def _get_decision(features):
    dtypes = {"Age":"float32","Total_Purchase":"float32","Account_Manager":"object","Years":"float32","Num_Sites":"float32","Location":"object","Company":"object","Onboard_Year":"float32","Onboard_Month":"float32","Onboard_DayOfMonth":"float32","Onboard_DayOfWeek":"float32","Onboard_Quarter":"float32","Onboard_WeekOfYear":"float32","Churned_Before":"object"}
    typed_features = {col: np.array(features[col], ndmin=2).astype(dtypes[col]) for col in dtypes}
    return PipelineModel.transform(typed_features)

class PipelineModel:
    def transform(features):
        storage_client = storage.Client()
        bucket = storage_client.bucket(f"{os.environ['DATA_BUCKET']}")
        blob = bucket.blob('model/onnx_pipeline.onnx')
        onnx_loaded = blob.download_as_string()
        sess = onnxruntime.InferenceSession(onnx_loaded)
        output = int(sess.run(None, features)[0][0])
        return output