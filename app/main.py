from fastapi import FastAPI
from pydantic import BaseModel
from app.model.model import predict_pipeline, predict_proba_pipeline
import joblib
import pandas as pd
import numpy as np

app = FastAPI()

#Preprocess test data
def preprocess_test_data(test_data):
    test_data = test_data.copy()
    
    test_data.rename(columns={'Unnamed: 0': 'booking_id'}, inplace=True)
    
    useless_cols = ['days_in_waiting_list', 'arrival_date_year', 'assigned_room_type', 'booking_changes', 'reservation_status', 'country', 'days_in_waiting_list']
    test_data.drop(columns=useless_cols, inplace=True)
    
    test_data.fillna(-1, inplace=True)
    
    test_data["arrival_date"] = pd.to_datetime(test_data["arrival_date"])
    test_data["booking_date"] = pd.to_datetime(test_data["booking_date"])
    
    if 'reservation_status_date' in test_data.columns:
        test_data['reservation_status_date'] = pd.to_datetime(test_data['reservation_status_date'])
        test_data['year'] = test_data['reservation_status_date'].dt.year
        test_data['month'] = test_data['reservation_status_date'].dt.month
        test_data['day'] = test_data['reservation_status_date'].dt.day
        test_data.drop(['reservation_status_date', 'arrival_date_month'], axis=1, inplace=True)
    
    mappings = {
        'hotel': {'Resort Hotel': 0, 'City Hotel': 1},
        'meal': {'BB': 0, 'FB': 1, 'HB': 2, 'SC': 3, 'Undefined': 4},
        'market_segment': {'Direct': 0, 'Corporate': 1, 'Online TA': 2, 'Offline TA/TO': 3, 'Complementary': 4, 'Groups': 5, 'Undefined': 6, 'Aviation': 7},
        'distribution_channel': {'Direct': 0, 'Corporate': 1, 'TA/TO': 2, 'Undefined': 3, 'GDS': 4},
        'reserved_room_type': {'C': 0, 'A': 1, 'D': 2, 'E': 3, 'G': 4, 'F': 5, 'H': 6, 'L': 7, 'B': 8},
        'deposit_type': {'No Deposit': 0, 'Refundable': 1, 'Non Refund': 3},
        'customer_type': {'Transient': 0, 'Contract': 1, 'Transient-Party': 2, 'Group': 3},
        'year': {2015: 0, 2014: 1, 2016: 2, 2017: 3}
    }
    
    for col, mapping in mappings.items():
        if col in test_data.columns:
            test_data[col] = test_data[col].map(mapping)
            test_data[col] = test_data[col].fillna(-1)
    
    num_cols = ['lead_time', 'arrival_date_week_number', 'arrival_date_day_of_month', 'agent', 'adr']
    for col in num_cols:
        if col in test_data.columns:
            test_data[col] = np.log(test_data[col] + 1)
    
    columns_to_drop = ['booking_id', 'arrival_date', 'booking_date']
    test_data.drop(columns=columns_to_drop, inplace=True)
    
    return test_data

class InputData(BaseModel):
    data: dict

class PredictionOut(BaseModel):
    prediction: str
    probability: float

@app.get("/")
def home():
    return {"health_check": "OK"}

@app.post("/predict", response_model=PredictionOut)
def predict(payload: InputData):

    class_labels = ["Not Canceled", "Canceled"]

    Fdata = pd.DataFrame.from_dict(payload.data, orient='index').transpose()

    # Make predictions
    prediction = predict_pipeline(Fdata)[0]
    probability = predict_proba_pipeline(Fdata)[0, prediction]

    prediction_label = class_labels[prediction]

    return {"prediction": prediction_label, "probability": float(probability)}

