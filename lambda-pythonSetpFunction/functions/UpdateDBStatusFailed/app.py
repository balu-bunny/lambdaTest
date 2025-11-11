import boto3
import os
import json
dynamodb = boto3.resource("dynamodb")
TABLE_NAME = 'qpms-backup'#os.environ.get("BACKUP_STATUS_TABLE")
table = dynamodb.Table(TABLE_NAME)

def lambda_handler(event, context):
    try:
        job_id = event.get("jobId")
        object_name = event.get("objectName")

        # Check state (completed or failed)
        state = event.get("status", {}).get("state", "EmptyStatus")

        table.put_item(
            Item={
                "Id": job_id,
                "jobId": job_id,
                "objectName": object_name,
                "status": state
            }
        )

        return {"jobId": job_id,
                "requestDetails": event.get("requestDetails", {}),
                "status": state}
    except Exception as e:
        print(f"Error updating DynamoDB for failed job: {e}")
        return {
            "status": "Error",
            "statusCode": 500,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*"
            },
            "body": json.dumps({"error": str(e)})
        }