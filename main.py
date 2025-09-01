# app.py
import json
import os
import time
import uuid
import logging
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional

import boto3
import requests
from botocore.exceptions import ClientError

# ---------- Logging ----------
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ---------- Env Vars ----------
SF_INSTANCE_URL = os.getenv("SF_INSTANCE_URL")               # e.g. https://yourinstance.my.salesforce.com
SF_API_VERSION = os.getenv("SF_API_VERSION", "v60.0")
SF_AUTH_METHOD = os.getenv("SF_AUTH_METHOD", "token")        # token | oauth
SF_ACCESS_TOKEN = os.getenv("SF_ACCESS_TOKEN")               # if SF_AUTH_METHOD == token
SF_OAUTH_CLIENT_ID = os.getenv("SF_OAUTH_CLIENT_ID")
SF_OAUTH_CLIENT_SECRET = os.getenv("SF_OAUTH_CLIENT_SECRET")
SF_OAUTH_REFRESH_TOKEN = os.getenv("SF_OAUTH_REFRESH_TOKEN")
SF_OAUTH_TOKEN_URL = os.getenv("SF_OAUTH_TOKEN_URL")         # e.g. https://login.salesforce.com/services/oauth2/token

S3_BUCKET = os.getenv("S3_BUCKET")                           # target bucket for backups
S3_PREFIX = os.getenv("S3_PREFIX", "salesforce-backups")

DDB_TABLE_NAME = os.getenv("DDB_TABLE_NAME", "SalesforceBackupJobs")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
TIMEOUT_DOWNLOAD_SECS = int(os.getenv("TIMEOUT_DOWNLOAD_SECS", "900"))  # 15 min

# ---------- AWS Clients ----------
s3 = boto3.client("s3", region_name=AWS_REGION)
dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
ddb_table = dynamodb.Table(DDB_TABLE_NAME)

# ---------- Helpers ----------
class TransientError(RuntimeError):
    pass

def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _require_env(name: str):
    val = os.getenv(name)
    if not val:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return val

def _get_sf_access_token() -> str:
    """
    Returns a valid Salesforce bearer token.
    Supports:
      - Static token (best via Secrets Manager -> env injection at deploy)
      - OAuth refresh token flow
    """
    if SF_AUTH_METHOD == "token":
        if not SF_ACCESS_TOKEN:
            raise RuntimeError("SF_ACCESS_TOKEN not set while SF_AUTH_METHOD=token")
        return SF_ACCESS_TOKEN

    # OAuth refresh-token flow
    for attempt in range(2):
        resp = requests.post(
            SF_OAUTH_TOKEN_URL,
            data={
                "grant_type": "refresh_token",
                "client_id": SF_OAUTH_CLIENT_ID,
                "client_secret": SF_OAUTH_CLIENT_SECRET,
                "refresh_token": SF_OAUTH_REFRESH_TOKEN,
            },
            timeout=20,
        )
        if resp.status_code == 200:
            data = resp.json()
            return data["access_token"]
        logger.warning("Salesforce token refresh failed (%s): %s", resp.status_code, resp.text)
        time.sleep(1)
    raise RuntimeError("Unable to obtain Salesforce access token")

def sf_request(method: str, path: str, **kwargs) -> requests.Response:
    """
    Minimal Salesforce REST wrapper with 401 retry (token refresh).
    path: '/services/data/vXX.X/...'
    """
    base = SF_INSTANCE_URL.rstrip("/")
    url = f"{base}{path}"
    token = _get_sf_access_token()
    headers = kwargs.pop("headers", {})
    headers["Authorization"] = f"Bearer {token}"
    headers.setdefault("Content-Type", "application/json")
    try:
        resp = requests.request(method, url, headers=headers, timeout=60, **kwargs)
        if resp.status_code == 401 and SF_AUTH_METHOD == "oauth":
            # refresh once and retry
            headers["Authorization"] = f"Bearer {_get_sf_access_token()}"
            resp = requests.request(method, url, headers=headers, timeout=60, **kwargs)
        return resp
    except requests.RequestException as e:
        raise TransientError(f"Network error calling Salesforce: {e}") from e

def ddb_put_status(object_name: str, job_id: str, state: str, extra: Optional[Dict[str, Any]]=None):
    item = {
        "pk": f"{object_name}#{job_id}",   # Composite primary key (use pk as the table's partition key)
        "objectName": object_name,
        "jobId": job_id,
        "state": state,
        "updatedAt": _utc_now_iso(),
    }
    if extra:
        item.update(extra)
    ddb_table.put_item(Item=item)

def safe_s3_key(*parts: str) -> str:
    joined = "/".join([p.strip("/").replace("..","") for p in parts if p])
    return joined

# ---------- 1) GetSalesforceObjectList ----------
def get_object_list_handler(event, context):
    """
    Input : {}
    Output: ["Account", "Contact", ...]  -> stored by Step Functions at $.objectList
    You can fetch from config/DB; here we hardcode and/or read from ENV.
    """
    logger.info("GetObjectList event: %s", json.dumps(event))
    # Option A: hard-coded or env
    from_env = os.getenv("SF_OBJECT_LIST")  # e.g.: Account,Contact,Opportunity
    if from_env:
        objects = [o.strip() for o in from_env.split(",") if o.strip()]
    else:
        # Option B: default set
        objects = ["Account", "Contact", "Opportunity"]

    # Option C: (optional) fetch from a DynamoDB config table or SSM parameter store

    logger.info("Returning object list: %s", objects)
    return objects

# ---------- 2) InitBulkBackup ----------
def init_bulk_backup_handler(event, context):
    """
    Input : { "objectName": "Account", ... }
    Output: { "jobId": "...", "objectName": "...", "createdAt": "..." }
            -> stored by Step Functions at $.backupJob
    Creates a (fake/demo) bulk export job via Salesforce Bulk API 2.0.
    Replace endpoints with your org-specific ones.
    """
    logger.info("InitBulkBackup event: %s", json.dumps(event))
    object_name = event["objectName"]

    # Example: create a bulk query job (adjust for your backup/export strategy)
    path = f"/services/data/{SF_API_VERSION}/jobs/query"
    body = {
        "operation": "queryAll",
        "query": f"SELECT+Id+FROM+{object_name}",
        "contentType": "CSV"
    }
    resp = sf_request("POST", path, data=json.dumps(body))
    if resp.status_code not in (200, 201):
        logger.error("Salesforce create job failed: %s %s", resp.status_code, resp.text)
        raise TransientError(f"Create job failed: {resp.text}")

    job = resp.json()
    job_id = job.get("id") or job.get("jobId") or str(uuid.uuid4())

    # Save initial status to DB
    ddb_put_status(object_name, job_id, "Created", {"salesforceJob": job})

    result = {
        "jobId": job_id,
        "objectName": object_name,
        "createdAt": _utc_now_iso()
    }
    logger.info("InitBulkBackup result: %s", result)
    return result

# ---------- 3) CheckBackupStatus ----------
def check_backup_status_handler(event, context):
    """
    Input : includes $.backupJob { jobId, objectName, ... }
    Output: { "state": "Completed|InProgress|Failed", "downloadUrls": [ ... ] }
            -> stored at $.status
    Polls Salesforce job status and returns standardized state + artifact URLs (if any).
    """
    logger.info("CheckBackupStatus event: %s", json.dumps(event))
    backup_job = event.get("backupJob", {})
    job_id = backup_job["jobId"]
    object_name = backup_job["objectName"]

    # Example: check job status
    path = f"/services/data/{SF_API_VERSION}/jobs/query/{job_id}"
    resp = sf_request("GET", path)
    if resp.status_code != 200:
        logger.error("Salesforce get job failed: %s %s", resp.status_code, resp.text)
        raise TransientError(f"Get job failed: {resp.text}")

    job = resp.json()
    sf_state = (job.get("state") or job.get("status") or "").lower()

    if sf_state in ("jobcomplete", "completed", "success"):
        # Example: list results to get downloadable parts
        results_path = f"/services/data/{SF_API_VERSION}/jobs/query/{job_id}/results"
        results_resp = sf_request("GET", results_path, headers={"Accept": "application/json"})
        download_urls: List[str] = []
        if results_resp.status_code == 200:
            # Some orgs expose signed URLs; others require follow-up download calls.
            # Here we assume API returns a list of parts with downloadUrl fields.
            data = results_resp.json()
            if isinstance(data, list):
                for part in data:
                    if "downloadUrl" in part:
                        download_urls.append(part["downloadUrl"])

        status = {"state": "Completed", "downloadUrls": download_urls}
    elif sf_state in ("aborted", "failed", "error"):
        status = {"state": "Failed", "error": job}
    else:
        status = {"state": "InProgress"}

    # Update DDB heartbeat
    ddb_put_status(object_name, job_id, status["state"], {"lastSalesforceStatus": job})
    logger.info("CheckBackupStatus returning: %s", status)
    return status

# ---------- 4) DownloadDataToS3 ----------
def download_data_to_s3_handler(event, context):
    """
    Input : includes $.status.downloadUrls, $.backupJob { jobId, objectName }
    Output: { "s3Keys": ["..."] } -> stored at $.downloadResult
    Downloads each artifact and uploads to S3.
    """
    logger.info("DownloadDataToS3 event: %s", json.dumps(event))
    backup_job = event["backupJob"]
    object_name = backup_job["objectName"]
    job_id = backup_job["jobId"]
    urls = event.get("status", {}).get("downloadUrls", [])

    if not urls:
        # Fallback: some Bulk APIs return results stream directly without URLs; you might need to GET results and stream body.
        # Here we no-op if nothing to download.
        logger.warning("No download URLs provided; skipping download.")
        ddb_put_status(object_name, job_id, "Completed", {"note": "No artifacts to download"})
        return {"s3Keys": []}

    deadline = time.time() + TIMEOUT_DOWNLOAD_SECS
    uploaded_keys: List[str] = []

    for idx, url in enumerate(urls, start=1):
        if time.time() > deadline:
            raise TransientError("Download timed out")

        # If URLs are relative to SF instance, prefix with instance URL
        if url.startswith("/"):
            url = f"{SF_INSTANCE_URL.rstrip('/')}{url}"

        # Download (bearer sometimes required)
        headers = {}
        if "salesforce.com" in url or "force.com" in url:
            headers["Authorization"] = f"Bearer {_get_sf_access_token()}"

        r = requests.get(url, headers=headers, stream=True, timeout=120)
        if r.status_code != 200:
            logger.error("Failed to download %s: %s %s", url, r.status_code, r.text[:200])
            raise TransientError(f"Failed to download artifact: {url}")

        key = safe_s3_key(S3_PREFIX, object_name, job_id, f"part-{idx}.csv")
        s3.upload_fileobj(r.raw, S3_BUCKET, key)
        uploaded_keys.append(key)
        logger.info("Uploaded to s3://%s/%s", S3_BUCKET, key)

    # Update DDB
    ddb_put_status(object_name, job_id, "Downloaded", {"s3Keys": uploaded_keys})
    return {"s3Keys": uploaded_keys}

# ---------- 5) UpdateDBStatusCompleted ----------
def update_db_status_completed_handler(event, context):
    """
    Input : has $.backupJob {objectName, jobId} and $.downloadResult {s3Keys}
    Output: { "ok": true }
    """
    logger.info("UpdateDBStatusCompleted event: %s", json.dumps(event))
    backup_job = event["backupJob"]
    object_name = backup_job["objectName"]
    job_id = backup_job["jobId"]
    download_result = event.get("downloadResult", {})
    extra = {
        "completedAt": _utc_now_iso(),
        "s3Keys": download_result.get("s3Keys", []),
    }
    ddb_put_status(object_name, job_id, "Completed", extra)
    return {"ok": True}

# ---------- 6) UpdateDBStatusFailed ----------
def update_db_status_failed_handler(event, context):
    """
    Input : has $.backupJob {objectName, jobId} and $.status.error (optional)
    Output: { "ok": true }
    """
    logger.info("UpdateDBStatusFailed event: %s", json.dumps(event))
    backup_job = event.get("backupJob", {})
    object_name = backup_job.get("objectName", "UNKNOWN")
    job_id = backup_job.get("jobId", str(uuid.uuid4()))
    status = event.get("status", {})
    error = status.get("error") or event.get("error") or {}
    extra = {
        "failedAt": _utc_now_iso(),
        "error": error,
    }
    ddb_put_status(object_name, job_id, "Failed", extra)
    return {"ok": True}
