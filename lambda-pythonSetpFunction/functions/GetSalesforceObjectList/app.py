import json
import requests
from sf_utils import getOrganizationDetails
def lambda_handler(event, context):
    print('-----------------init---------------------')
    try:
        SALESFORCE_URL, ACCESS_TOKEN, version = getOrganizationDetails(event.get("requestDetails", {}).get("orgId"))
        url = f"{SALESFORCE_URL}/services/data/{version}/sobjects/"
        headers = {"Authorization": f"Bearer {ACCESS_TOKEN}", "Content-Type": "application/json"}

        response = requests.get(url, headers=headers)
        response.raise_for_status()
        job_response = response.json()

        object_list = [
            obj["name"]
            for obj in job_response.get("sobjects", [])
            if obj.get("name", "").endswith("__c")
        ]


        object_list_additional = [
                'Account',
                'Contact',
                'Task',
                'Event',
                'Note',
                'Attachment',
                'Document',
                'Report',
                'Dashboard',
                'ProcessDefinition',
                'ProcessNode',
                'ProcessInstance',
                'ProcessInstanceStep',
                'ProcessInstanceWorkitem',
                'ContentVersion',
                'ContentDocument',
                'ContentDocumentLink',
                'ContentWorkspace',
            ]
        #'ContentWorkspaceDoc'
        object_list.extend(object_list_additional)
        print(f"Salesforce objects to backup: {object_list}")
        print(f"Event: {event}")
        print(f"Context: {context}")
        if "httpMethod" in event:  
            print(f"httpMethod: {event}")
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
        else:
            print(f"else: {event}")
            return  { 
                     "objects": object_list,
                     "requestDetails": event.get("requestDetails", {})
                     }
            #return ['ContentVersion']

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
