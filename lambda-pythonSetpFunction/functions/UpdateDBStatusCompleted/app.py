import boto3
import os

dynamodb = boto3.resource("dynamodb")
TABLE_NAME = os.environ.get("BACKUP_STATUS_TABLE")
table = dynamodb.Table(TABLE_NAME)

def lambda_handler(event, context):
    job_id = event["backupJob"]["jobId"]
    object_name = event["backupJob"]["objectName"]

    # Check state (completed or failed)
    state = event.get("status", {}).get("state", "Completed")

    table.put_item(
        Item={
            "jobId": job_id,
            "objectName": object_name,
            "status": state
        }
    )

    return {"jobId": job_id, "status": state}
