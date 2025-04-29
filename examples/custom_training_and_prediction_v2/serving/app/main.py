# Import the required libraries
from fastapi import FastAPI, Request
import joblib
import json
import os
from google.cloud import storage
import logging

app = FastAPI()
# Define the Cloud Storage client
gcs_client = storage.Client()

# Download the model file from Cloud Storage bucket
# For local testing comment GCS loading, next line loads from local drive
with open("model.pkl", 'wb') as model_f:
    gcs_client.download_blob_to_file(
            f"{os.environ['AIP_STORAGE_URI']}/model.pkl", model_f
        )
    
# Load the scikit-learn model/pipeline file
_model = joblib.load("model.pkl")
logging.info("Model loaded!")

# Define a function for health route
@app.get(os.environ['AIP_HEALTH_ROUTE'], status_code=200)
def health():
    return "OK"

# Define a function for prediction route
@app.post(os.environ['AIP_PREDICT_ROUTE'])
async def predict(request: Request):
    body = await request.json()
    # parse the request instances
    instances = body["instances"]
    logging.info(body.get("parameters", {}).get("columns", {}))
    # pass it to the model/pipeline for prediction scores
    predictions = _model.predict_proba(instances).tolist()
    # return the batch prediction scores
    return {"predictions": predictions}
