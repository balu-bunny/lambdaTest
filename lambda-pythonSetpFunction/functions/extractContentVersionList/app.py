import boto3
import csv
import json
import io
S3BUCKET = 'qpms-backup'#os.environ.get("S3_BUCKET")
def lambda_handler(event, context):
    try:
        s3 = boto3.client('s3')
        print(f"Event Received: {event}")
        # Get input parameters
        global S3BUCKET
        S3_BUCKET = S3BUCKET
        S3_KEY = event.get('s3Key')
        COLUMN_NAME = 'Id'  # specify which column to extract
        COLUMN_FILE = 'PathOnClient'
        # if not (S3_BUCKET and S3_KEY and COLUMN_NAME):
        #     raise ValueError("Missing required parameters: s3_bucket, s3_key, or column_name")

        # --- 1️⃣ Download CSV file from S3 ---
        response = s3.get_object(Bucket=S3_BUCKET, Key=S3_KEY)
        csv_content = response['Body'].read().decode('utf-8')
        print(f"Downloaded CSV content from s3://{S3_BUCKET}/{S3_KEY}")
        # --- 2️⃣ Parse CSV and extract column ---
        csv_reader = csv.DictReader(io.StringIO(csv_content))
        column_values = [f"{row[COLUMN_NAME]}/{row[COLUMN_FILE]}" for row in csv_reader if COLUMN_NAME in row]
        print(f"Extracted {len(column_values)} values from column '{COLUMN_NAME}'")
        # --- 3️⃣ Detect if API Gateway triggered this ---
        if "httpMethod" in event:
            return {
                "statusCode": 200,
                "headers": {
                    "Content-Type": "application/json",
                    "Access-Control-Allow-Origin": "*"
                },
                "body": json.dumps({
                    "message": f"Extracted column '{COLUMN_NAME}' successfully",
                    "values": column_values
                })
            }
        else:
            # Direct Lambda invocation (e.g. from Step Function)
            return {"column_values": column_values,
                    "s3_key": S3_KEY,"S3BUCKET":S3BUCKET,
                    "requestDetails": event.get("requestDetails", {})
                    }

    except Exception as e:
        print(f"Error: {e}")
        return {
            "statusCode": 500,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*"
            },
            "body": json.dumps({"error": str(e)})
        }
