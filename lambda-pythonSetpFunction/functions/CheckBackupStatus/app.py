import json
import requests
import os

SALESFORCE_URL = os.environ.get("SALESFORCE_URL")
ACCESS_TOKEN = os.environ.get("SALESFORCE_ACCESS_TOKEN")

def lambda_handler(event, context):
    job_id = event["backupJob"]["jobId"]

    url = f"{SALESFORCE_URL}/services/data/v60.0/jobs/ingest/{job_id}"
    headers = {"Authorization": f"Bearer {ACCESS_TOKEN}"}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    job_status = response.json()

    return {
        "jobId": job_id,
        "state": job_status["state"]
    }
