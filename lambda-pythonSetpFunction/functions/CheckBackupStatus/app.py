import json
import requests
import os
from sf_utils import getOrganizationDetails
# SALESFORCE_URL = os.environ.get("SALESFORCE_URL")
# ACCESS_TOKEN = os.environ.get("SALESFORCE_ACCESS_TOKEN")
def lambda_handler(event, context):
    
    try:
        object_name = event.get("objectName")
        job_id = event.get("jobId")
        if not job_id:
            raise ValueError(f"Job ID not provided for object {object_name}")
        SALESFORCE_URL, ACCESS_TOKEN, version = getOrganizationDetails(event.get("requestDetails", {}).get("orgId"))
        url = f"{SALESFORCE_URL}/services/data/{version}/jobs/query/{job_id}"
        headers = {"Authorization": f"Bearer {ACCESS_TOKEN}", "Content-Type": "application/json"}

        response = requests.get(url, headers=headers)
        response.raise_for_status()
        job_status = response.json()

        return {
            "jobId": job_id,
            "objectName": object_name,
            "state": job_status["state"],
            "requestDetails": event.get("requestDetails", {})
        }
    except Exception as e:
        print(f"Error retrieving Salesforce object list: {e}")
        return {
            "state": "Failed",
            "statusCode": 500,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*"
            },
            "body": json.dumps({"error": str(e)})
        }