import boto3
import os

dynamodb = boto3.resource("dynamodb")
TABLE_NAME = 'qpms-backup'#os.environ.get("BACKUP_STATUS_TABLE")
table = dynamodb.Table(TABLE_NAME)

def lambda_handler(event, context):
    job_id = event.get("jobId")
    # object_name = event["backupJob"]["objectName"]

    # # Check state (completed or failed)
    state = event.get("status")

    # table.put_item(
    #     Item={
    #         "Id": job_id,
    #         "jobId": job_id,
    #         "objectName": object_name,
    #         "status": state
    #     }
    # )

    return {"jobId": job_id,
            "requestDetails": event.get("requestDetails", {}),
            "status": state}
