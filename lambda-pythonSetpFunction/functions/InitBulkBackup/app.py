import json
import requests
import os

SALESFORCE_URL = os.environ.get("SALESFORCE_URL")
ACCESS_TOKEN = os.environ.get("SALESFORCE_ACCESS_TOKEN")

def lambda_handler(event, context):
    object_name = event["objectName"]

    # Call Salesforce Bulk API to create job
    url = f"{SALESFORCE_URL}/services/data/v60.0/jobs/ingest"
    headers = {"Authorization": f"Bearer {ACCESS_TOKEN}", "Content-Type": "application/json"}

    payload = {
        "object": object_name,
        "operation": "query",
        "lineEnding": "LF",
        "concurrencyMode": "Parallel"
    }

    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    job_info = response.json()

    # Example: return jobId for tracking
    return {
        "objectName": object_name,
        "jobId": job_info["id"],
        "state": job_info["state"]
    }
