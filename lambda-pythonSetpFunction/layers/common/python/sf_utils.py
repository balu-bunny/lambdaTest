import os
import json
import requests
import boto3
global_var = None

def _extract_access_token_from_response(resp):
    # resp may be dict, string (json), or nested with 'body'
    if resp is None:
        return None
    if isinstance(resp, str):
        try:
            resp = json.loads(resp)
        except Exception:
            return None
    if isinstance(resp, dict):
        # direct access_token
        if 'access_token' in resp:
            return resp['access_token']
        # nested body (string or dict)
        if 'body' in resp:
            return _extract_access_token_from_response(resp['body'])
        # common nested patterns
        for key in ('token', 'data', 'result'):
            if key in resp and isinstance(resp[key], dict) and 'access_token' in resp[key]:
                return resp[key]['access_token']
    return None

def get(url, method="GET", headers=None, payload=None):
    try:
        if method == "GET":
            response = requests.get(url, headers=headers, timeout=10)
        else:
            response = requests.post(url, headers=headers, json=payload, timeout=10)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"HTTP request error: {e}")
        return None

def post(url, headers=None, payload=None):
    return get(url, method="POST", headers=headers, payload=payload)

def getOrganizationDetails(orgId):
    url = get_url(orgId)
    token = get_access_token(orgId)
    return url, token, 'v65.0'

def get_url(OrgId=None):
    if OrgId == 'qualityzeqms.my.salesforce.com':
        return 'https://qualityzeqms.my.salesforce.com'
    return 'https://qpmsint2-dev-ed.my.salesforce.com'

def get_access_token(OrgId=None):
    # global global_var
    
    
    
    # # If cached and valid, return
    # if isinstance(global_var, dict):
    #     token = _extract_access_token_from_response(global_var)
    #     if token:
    #         return token

    # # generate and cache
    token_data = generate_access_token(OrgId)
    print(f"Generated token data: {token_data}")
    # token = _extract_access_token_from_response(token_data)
    # if token:
    #     # store a normalized dict
    #     global_var = {'access_token': token}
    #     return token
    token = _extract_access_token_from_response(token_data)

    return token

def generate_access_token(OrgId=None):
    """
    Try client_credentials first, then password grant if client_credentials not supported.
    Returns a dict or parsed response (not string).
    """
    
    #client_id = os.environ.get('SF_CLIENT_ID')
    # secrets_client = boto3.client('secretsmanager')
    # secret = secrets_client.get_secret_value(SecretId='MyAppClientSecret')
    # credentials = json.loads(secret['SecretString'])

    # client_id = credentials.get('client_id')
    # client_secret = credentials.get('client_secret')
    
    client_id = ''
    client_secret = ''

    if OrgId == 'qualityzeqms.my.salesforce.com':
        client_id = ''
        client_secret = ''
        print("Using qualityzeqms credentials")
    
    #client_secret = os.environ.get('SF_CLIENT_SECRET')
    username = os.environ.get('SF_USERNAME')
    password = os.environ.get('SF_PASSWORD')
    url = get_url(OrgId)
    token_url = os.environ.get('SF_TOKEN_URL', f'{url}/services/oauth2/token')

    # prefer client_credentials if client_id & client_secret present
    if client_id and client_secret and not (username and password):
        payload = {
            'grant_type': 'client_credentials',
            'client_id': client_id,
            'client_secret': client_secret
        }
    else:
        # fallback to password grant (common for Salesforce integrations)
        if not (client_id and client_secret and username and password):
            print("Missing credentials for token generation (set SF_CLIENT_ID, SF_CLIENT_SECRET, SF_USERNAME, SF_PASSWORD)")
            return None
        payload = {
            'grant_type': 'password',
            'client_id': client_id,
            'client_secret': client_secret,
            'username': username,
            'password': password
        }

    try:
        resp = requests.post(token_url, data=payload, timeout=10)
        resp.raise_for_status()
        try:
            print("Token response JSON:", resp.json())
            return resp.json()
        except ValueError:
            # sometimes libraries return text; try parse
            try:
                return json.loads(resp.text)
            except Exception:
                return {'body': resp.text}
    except Exception as e:
        print(f"generate_access_token error: {e}")
        return None

if __name__ == "__main__":
    print("token:", get_access_token())