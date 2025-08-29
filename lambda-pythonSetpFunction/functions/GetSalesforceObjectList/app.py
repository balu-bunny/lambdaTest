import json

def lambda_handler(event, context):
    try:
        object_list = ["Account", "Contact", "Opportunity"]
        print(f"Salesforce objects to backup: {object_list}")

        return {
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*"   # <-- needed for browser (CORS)
            },
            "body": json.dumps({
                "message": "Hello",
                "objects": object_list,
                "event": event,   # event is already JSON-serializable
                "context": {
                    "function_name": context.function_name,
                    "function_version": context.function_version,
                    "memory_limit_in_mb": context.memory_limit_in_mb,
                    "aws_request_id": context.aws_request_id,
                    "log_group_name": context.log_group_name,
                    "log_stream_name": context.log_stream_name
                }
            })
        }

    except Exception as e:
        print(f"Error retrieving Salesforce object list: {e}")
        return {
            "statusCode": 500,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*"
            },
            "body": json.dumps({"error": str(e)})
        }
