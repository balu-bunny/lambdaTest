import boto3
import requests
import os

s3 = boto3.client("s3")
SALESFORCE_URL = os.environ.get("SALESFORCE_URL")
ACCESS_TOKEN = os.environ.get("SALESFORCE_ACCESS_TOKEN")
S3_BUCKET = os.environ.get("S3_BUCKET")

def lambda_handler(event, context):
    job_id = event["backupJob"]["jobId"]
    object_name = event["backupJob"]["objectName"]

    url = f"{SALESFORCE_URL}/services/data/v60.0/jobs/ingest/{job_id}/successfulResults/"
    headers = {"Authorization": f"Bearer {ACCESS_TOKEN}"}
    response = requests.get(url, headers=headers, stream=True)
    response.raise_for_status()

    # Save to S3
    s3_key = f"salesforce_backups/{object_name}/{job_id}.csv"
    s3.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=response.content)

    return {
        "jobId": job_id,
        "objectName": object_name,
        "s3Key": s3_key
    }
