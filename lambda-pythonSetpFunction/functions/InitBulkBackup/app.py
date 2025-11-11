import json
import requests
import os
from sf_utils import getOrganizationDetails
def lambda_handler(event, context):
    try:
        object_name = event["objectName"]   
        
        domainUrl, access_token, version = getOrganizationDetails(event.get("requestDetails", {}).get("orgId"))
        backup_type = event.get("requestDetails", {}).get("BackUpType")

        if checkIfQueryRowsAreNotEmpty(domainUrl,access_token,version,object_name,backup_type) == False:
            return {
                "status": "Skipped",
                "objectName": object_name,
                "jobId": None,
                "state": "Aborted",
                "requestDetails": event.get("requestDetails", {})
            }


        #object_name = "Account"
        # Call Salesforce Bulk API to create job
        url = f"{domainUrl}/services/data/{version}/jobs/query"
        headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
        query = get_object_query(object_name, domainUrl, access_token, backup_type)
        #f"SELECT Id, Name FROM {object_name}

        payload = {
            "operation": "query",
            "query": query
        }
        print(f"Creating bulk query job for object: {object_name}")
        print(f"Payload: {payload}")

        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        job_info = response.json()

        # Example: return jobId for tracking
        return {
            "status": "Submitted",
            "objectName": object_name,
            "jobId": job_info["id"],
            "state": job_info["state"],
            "requestDetails": event.get("requestDetails", {})
        }
    except Exception as e:
        print(f"Error retrieving Salesforce object list: {e}")
        return {
            "status": "Error",
            "statusCode": 500,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*"
            },
            "body": json.dumps({"error": str(e)})
        }

def get_object_query(object_name, domainUrl, access_token, backup_type="Daily"):

        headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
        url = f"{domainUrl}/services/data/v60.0/sobjects/{object_name}/describe"

        response = requests.get(url, headers=headers)
        response.raise_for_status()
        job_response = response.json()

        #field_names = [field.get("name") for field in job_response.get("fields", []) if "name" in field]
        object_fields = job_response

        print(f"Object Fields: {object_fields}")
                
        compound_parents = {
            f["compoundFieldName"]
            for f in object_fields.get("fields", [])
            if "compoundFieldName" in f and f["compoundFieldName"]
        }
        print(f"Compound Parents: {compound_parents}")
        # Step 2️⃣: Filter out fields whose name appears in compound_parents
        filtered_fields = [
            f["name"]
            for f in object_fields.get("fields", [])
            if f["name"] not in compound_parents
        ]
        if object_name == "ContentVersion":
            filtered_fields.remove("VersionData")  # Remove 'VersionData' field if present
        print(f"Filtered Fields: {filtered_fields}")
        url = f"SELECT {', '.join(filtered_fields)} FROM {object_name}"
    
    
        LastModifiedDate = 'SystemModstamp'

        if object_name.endswith('__b'):
            LastModifiedDate = 'CreatedDate'
            
        if backup_type == 'Daily':
            url += f" WHERE {LastModifiedDate} = YESTERDAY"
            
        return url
    

def checkIfQueryRowsAreNotEmpty(SALESFORCE_URL,ACCESS_TOKEN,version,objectName,backup_type):
    url = f"{SALESFORCE_URL}/services/data/{version}/query?q=SELECT+COUNT(ID)+FROM+{objectName}"
    
    LastModifiedDate = 'SystemModstamp'
    
    
    if objectName.endswith('__b') and backup_type == 'Full':
        return True    
    
        
    if objectName.endswith('__b'):
        LastModifiedDate = 'CreatedDate'
    
    if backup_type == 'Daily':
        url += f"+WHERE+{LastModifiedDate}+=+YESTERDAY"
    
    # if backup_type == 'Daily':
    #     url += "+WHERE+SystemModstamp+=+LAST_N_DAYS:1"
    print(f"Check Rows URL: {url}")
    
    
    headers = {"Authorization": f"Bearer {ACCESS_TOKEN}"}
    response = requests.get(url, headers=headers, stream=True)
    response.raise_for_status()
    data = response.json()
    print(f"Check Rows Response Data: {data}")
    result = data['records'][0]['expr0']
    
    return result>0    