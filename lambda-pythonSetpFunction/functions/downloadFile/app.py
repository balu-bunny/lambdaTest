import requests
import boto3
from sf_utils import getOrganizationDetails
def lambda_handler(event, context):
    try:
        SALESFORCE_URL, ACCESS_TOKEN, version = getOrganizationDetails(event.get("requestDetails", {}).get("orgId"))
        CONTENT_VERSION_ID = event['contentVersionId']
        S3_BUCKET = event['S3BUCKET']
        S3_KEY = event['s3Key']

        stream_salesforce_to_s3(
            instance_url=SALESFORCE_URL,
            content_version_id=CONTENT_VERSION_ID,
            access_token=ACCESS_TOKEN,
            bucket_name=S3_BUCKET,
            s3_key=S3_KEY
        )

        return {
            "statusCode": 200,
            "body": f"Successfully streamed ContentVersion {CONTENT_VERSION_ID} to s3://{S3_BUCKET}/{S3_KEY}",
            "requestDetails": event.get("requestDetails", {})
        }
    except Exception as e:
        print(f"Error: {e}")
        return {
            "statusCode": 500,
            "body": f"Error occurred: {str(e)}"
        }
def stream_salesforce_to_s3(instance_url, content_version_id, access_token, bucket_name, s3_key):
    """
    Streams large ContentVersion data directly from Salesforce to S3 without saving locally.
    """

    contentVersionId, fileName = content_version_id.split('/', 1)
    s3 = boto3.client("s3")
    url = f"{instance_url}/sfc/servlet.shepherd/version/download/{contentVersionId}"
    headers = {"Authorization": f"Bearer {access_token}"}

    print(f"📥 Streaming download from: {url}")

    # Final destination path in S3
    key = s3_key.removesuffix('.csv')  # Ensure no trailing slash
    location = f"{key}/{contentVersionId}_{fileName}"

    try:
        # Stream download from Salesforce
        with requests.get(url, headers=headers, stream=True, timeout=60) as response:
            response.raise_for_status()

            # Upload the streamed data directly to S3
            s3.upload_fileobj(response.raw, bucket_name, location)

        print(f"✅ Successfully uploaded to s3://{bucket_name}/{location}")

    except Exception as e:
        print(f"❌ Error occurred while streaming to S3: {e}")
        raise
# stream_salesforce_to_s3(
#     instance_url="https://qpmsint2-dev-ed.my.salesforce.com",
#     content_version_id="068Dn00000ABCDE",
#     access_token="00Dxx0000000000!AQ0...",
#     bucket_name="my-large-salesforce-backups",
#     s3_key="backups/ContentVersion_068Dn00000ABCDE.bin"
# )
