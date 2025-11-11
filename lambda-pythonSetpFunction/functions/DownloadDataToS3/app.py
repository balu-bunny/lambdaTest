import boto3
import requests
import os
import datetime as dt
s3 = boto3.client("s3")
#S3_BUCKET = os.environ.get("S3_BUCKET")
S3_BUCKET = 'qpms-backup'#os.environ.get("S3_BUCKET")
from sf_utils import getOrganizationDetails

def lambda_handler(event, context):
    print("Init.....")
    job_id = event.get("jobId")
    object_name = event.get("objectName")    
    SALESFORCE_URL, ACCESS_TOKEN, version = getOrganizationDetails(event.get("requestDetails", {}).get("orgId"))

    print("Downloading data for job:", job_id, "object:", object_name)
    backup_type = event.get("requestDetails", {}).get("BackUpType")

    print("Data exists for object:", object_name, "proceeding with download.")
    url = f"{SALESFORCE_URL}/services/data/{version}/jobs/query/{job_id}/results/"
    
    Sforce_Locator = event.get("Sforce_Locator", "")
    
    if Sforce_Locator:
        url += f"?locator={Sforce_Locator}"
    
    headers = {"Authorization": f"Bearer {ACCESS_TOKEN}"}
    response = requests.get(url, headers=headers, stream=True)
    response.raise_for_status()

    Sforce_Locator = response.headers.get("Sforce-Locator", "")
    Sforce_NumberOfRecords = response.headers.get("Sforce-NumberOfRecords", "")
    # Save to S3
    datetime = dt.datetime.now().strftime("%Y%m%d_%H%M%S") 
    date = dt.datetime.now().strftime("%Y%m%d")
    org = event.get("requestDetails", {}).get("orgId", "defaultOrg")
    s3_key = f"salesforce_backups/{org}/{date}/{object_name}/{job_id}_{datetime}_{Sforce_Locator}_{Sforce_NumberOfRecords}.csv"
    s3.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=response.content)


    return {
        "Sforce_Locator": Sforce_Locator,
        "Sforce_NumberOfRecords": Sforce_NumberOfRecords,
        "status": ("Completed" if Sforce_Locator else "Partial"),
        "jobId": job_id,
        "objectName": object_name,
        "s3Key": s3_key,
        "requestDetails": event.get("requestDetails", {})
    }

