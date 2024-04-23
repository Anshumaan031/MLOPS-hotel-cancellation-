import pickle
from pathlib import Path
import pandas as pd
import joblib

BASE_DIR = Path(__file__).resolve(strict=True).parent

with open(f"{BASE_DIR}/4301model.pkl", "rb") as f:
    model_pipeline = joblib.load(f)

def predict_pipeline(data):
    pred = model_pipeline.predict(data)
    return pred

def predict_proba_pipeline(data):
    pred_proba = model_pipeline.predict_proba(data)
    return pred_proba