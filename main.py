import os
import json
import requests
import time
from datetime import datetime
from requests.auth import HTTPBasicAuth

# ===========================
# CONFIGURATION
# ===========================
SF_CLIENT_ID = os.getenv("SF_CLIENT_ID")
SF_CLIENT_SECRET = os.getenv("SF_CLIENT_SECRET")
SF_TOKEN_URL = os.getenv("SF_TOKEN_URL")

SF_INSTANCE_DOMAIN = "https://globe.my.salesforce.com"
SF_API_VERSION = "v60.0"

# --- Jira Cloud / JSM ---
JIRA_URL = os.getenv("JIRA_URL")
EMAIL = os.getenv("EMAIL")
API_TOKEN = os.getenv("API_TOKEN")
JIRA_CLOUD_ID = os.getenv("JIRA_CLOUD_ID")

# ✅ TARGETED PROJECT KEYS
SERVICE_DESK_KEYS = ["MOBILE", "ERT", "SNDBX"]


# Shared JIRA Auth
AUTH = HTTPBasicAuth(EMAIL, API_TOKEN)
JIRA_HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
}

# ===========================
# SALESFORCE HELPERS
# ===========================
def get_salesforce_token():
    data = {
        "grant_type": "client_credentials",
        "client_id": SF_CLIENT_ID,
        "client_secret": SF_CLIENT_SECRET
    }
    r = requests.post(SF_TOKEN_URL, data=data)
    if not r.ok:
        raise Exception(f"Failed to get Salesforce token: {r.text}")
    js = r.json()
    return js["access_token"], js["instance_url"]

def soql(instance_url, access_token, query):
    url = f"{instance_url}/services/data/{SF_API_VERSION}/query"
    headers = {"Authorization": f"Bearer {access_token}"}
    r = requests.get(url, headers=headers, params={"q": query})
    if not r.ok:
        raise Exception(f"SOQL error: {r.text}")
    return r.json()

def get_recent_accounts(token, instance_url):
    """
    Pulls accounts modified TODAY.
    """
    query = f"""
    SELECT Id, Name, Industry, Type, B2B_Full_Address_2__c,
           Owner.Name, 
            B2B_Cluster__c, B2B_Area__c 
    FROM Account
    WHERE RecordType.DeveloperName = 'B2B_Accounts'
    AND EGFS1_Not_Active__c = false
    AND LastModifiedDate = TODAY
    """
    return soql(instance_url, token, query).get("records", [])

def get_account_contacts(token, instance_url, account_id):
    """
    ✅ UPDATED: Queries AccountContactRelation to find Direct & Indirect contacts.
    Checks for Authorized Signatory/Representative in BOTH Position and Role.
    """
    query = f"""
    SELECT ContactId, 
           Contact.Name, 
           Contact.Email, 
           Contact.Position__c, 
           Contact.Contact_Role__c,
           Contact.Phone, 
           Contact.MobilePhone
    FROM AccountContactRelation
    WHERE AccountId = '{account_id}'
    AND IsActive = true
    AND (
        Contact.Position__c LIKE '%Authorized Signatory%' 
        OR Contact.Position__c LIKE '%Authorized Representative%'
        OR Contact.Contact_Role__c INCLUDES ('Authorized Signatory', 'Authorized Representative')
    )
    """
    
    data = soql(instance_url, token, query).get("records", [])
    
    contacts = []
    for item in data:
        contact_data = item.get("Contact", {})
        contact_data["Id"] = item.get("ContactId")
        contacts.append(contact_data)
        
    return contacts

# ===========================
# JIRA HELPERS
# ===========================
def create_org(name):
    name = str(name).strip()
    url = f"{JIRA_URL}/rest/servicedeskapi/organization"
    
    # Attempt to create
    r = requests.post(url, headers=JIRA_HEADERS, auth=AUTH, json={"name": name})
    
    if r.status_code == 201:
        return r.json().get("id")

    # If it fails, check if it already exists
    if r.status_code in (400, 409):
        # print(f"   ℹ️ Org '{name}' might exist. Searching...") # Optional debug log
        org_id = find_org_id(name)
        if org_id:
            return org_id
        
        # If search also fails, print the CREATE error to understand why
        print(f"   ❌ Failed to create '{name}' (Status: {r.status_code})")
        print(f"      Response: {r.text}") # <--- THIS IS KEY
        return None

    # For other errors (401, 403, 500)
    print(f"   ❌ API Error creating '{name}': {r.status_code}")
    print(f"      Response: {r.text}")
    return None

def find_org_id(name):
    start = 0
    name = str(name).strip()
    while True:
        url = f"{JIRA_URL}/rest/servicedeskapi/organization"
        r = requests.get(url, headers=JIRA_HEADERS, auth=AUTH,
                         params={"start": start, "limit": 50})
        data = r.json()
        for org in data.get("values", []):
            if org["name"] == name:
                return org["id"]
        if data.get("isLastPage", True):
            break
        start = data["start"] + data["limit"]
    return None

def link_org_to_service_desks(org_id):
    """Links Org to MOBILE, ERT, and SNDBX."""
    for key in SERVICE_DESK_KEYS:
        url = f"{JIRA_URL}/rest/servicedeskapi/servicedesk/{key}/organization"
        r = requests.post(url, headers=JIRA_HEADERS, auth=AUTH, json={"organizationId": org_id})
        if r.status_code not in (204, 404):
             print(f"   ⚠️ Failed linking to '{key}': {r.status_code}")

def search_jira_user(email):
    url = f"{JIRA_URL}/rest/api/3/user/search"
    r = requests.get(url, headers=JIRA_HEADERS, auth=AUTH, params={"query": email})
    if not r.ok: return None
    users = r.json()
    for u in users:
        if u.get("emailAddress", "").lower() == email.lower():
            return u.get("accountId")
    return None

def create_jira_customer(name, email):
    url = f"{JIRA_URL}/rest/servicedeskapi/customer"
    r = requests.post(url, headers=JIRA_HEADERS, auth=AUTH,
                      json={"fullName": name, "email": email})
    if r.status_code == 201:
        return r.json().get("accountId")
    if r.status_code in (400, 409):
        return search_jira_user(email)
    return None

def add_users_to_org(org_id, ids):
    if not ids: return
    url = f"{JIRA_URL}/rest/servicedeskapi/organization/{org_id}/user"
    requests.post(url, headers=JIRA_HEADERS, auth=AUTH, json={"accountIds": ids})

# ===========================
# ROBUST UPDATE FUNCTIONS
# ===========================
def update_org_detail_field(org_id, field_name, value):
    if not value: return False

    url = f"https://api.atlassian.com/jsm/csm/cloudid/{JIRA_CLOUD_ID}/api/v1/organization/{org_id}/details"
    query = {'fieldName': field_name}
    payload = json.dumps({"values": [str(value)]})
    
    time.sleep(1) # Buffer for indexing
    
    for attempt in range(1, 4): # Retry 3 times
        try:
            r = requests.put(url, data=payload, headers=JIRA_HEADERS, params=query, auth=AUTH)
            if r.status_code == 200:
                print(f"   ✅ [ORG UPDATE] Success: {field_name}")
                return True
            if r.status_code == 404: 
                time.sleep(attempt * 1.5)
            elif r.status_code == 429: 
                time.sleep(5)
            else: 
                break
        except Exception as e:
            print(f"   ❌ Error updating Org Field {field_name}: {e} - {org_id}")
            time.sleep(1)
            
    print(f"   ⚠️ [ORG UPDATE] Failed: {field_name}")
    return False

def update_customer_detail_field(account_id, field_name, value):
    if not value: return False

    url = f"https://api.atlassian.com/jsm/csm/cloudid/{JIRA_CLOUD_ID}/api/v1/customer/{account_id}/details"
    query = {'fieldName': field_name}
    payload = json.dumps({"values": [str(value)]})
    
    time.sleep(0.5) # Buffer for indexing
    
    max_retries = 5
    for attempt in range(1, max_retries + 1):
        try:
            r = requests.put(url, data=payload, headers=JIRA_HEADERS, params=query, auth=AUTH)
            if r.status_code == 200: 
                print(f"   ✅ [CUSTOMER UPDATE] Success: {field_name}")
                return True
            elif r.status_code == 404: time.sleep(attempt * 2)
            elif r.status_code == 429: time.sleep(5)
            else: 
                print(f"   ⚠️ Failed {field_name}: {r.status_code} {r.text} - {account_id}")
                break
        except Exception as e:
            print(f"   ❌ Error updating Customer Field {field_name}: {e} - {account_id}")
            time.sleep(1)
            
    print(f"   ⚠️ [CUSTOMER UPDATE] Failed: {field_name}")
    return False

# ===========================
# PROCESS LOGIC
# ===========================
def process_single_account(acc, sf_token, sf_instance):
    try:
        acc_id = acc["Id"]
        acc_name = acc["Name"]
        print(f"➡️ Processing {acc_name} ({acc_id})")

        # 1. Org Logic
        org_id = create_org(acc_name)
        if not org_id:
            print("❌ Could not create/fetch org")
            return
        
        link_org_to_service_desks(org_id)

        # 2. Org Details
        update_org_detail_field(org_id, "Salesforce Account Id", acc_id)
        update_org_detail_field(org_id, "Company Name", acc_name)
        update_org_detail_field(org_id, "Company Address", acc.get("B2B_Full_Address_2__c"))
        update_org_detail_field(org_id, "Industry", acc.get("Industry"))
        update_org_detail_field(org_id, "Customer Type", "Customer")
        
        owner = acc.get("Owner")
        if owner and owner.get("Name"):
            update_org_detail_field(org_id, "Account Manager", owner.get("Name"))

        update_org_detail_field(org_id, "Sales Cluster", acc.get("B2B_Cluster__c"))
        update_org_detail_field(org_id, "Sales Area", acc.get("B2B_Area__c"))

        # 3. Contact Logic
        contacts = get_account_contacts(sf_token, sf_instance, acc_id)
        account_ids = []

        for c in contacts:
            email = str(c.get("Email") or "").strip()
            name = str(c.get("Name") or "").strip()
            phone = str(c.get("MobilePhone") or c.get("Phone") or "").strip()
            
            position_raw = str(c.get("Position__c") or "")
            role_raw = str(c.get("Contact_Role__c") or "")
            combined_roles = (position_raw + " " + role_raw).lower()

            if not email: continue

            # Priority Logic
            final_role = None
            if "authorized signatory" in combined_roles:
                final_role = "Authorized Signatory"
            elif "authorized representative" in combined_roles:
                final_role = "Authorized Representative"
            else:
                continue

            acct_id = create_jira_customer(name, email)

            if acct_id:
                account_ids.append(acct_id)
                # Strict Field Updates
                update_customer_detail_field(acct_id, "ROLE", final_role)
                update_customer_detail_field(acct_id, "Mobile Number", phone)
                update_customer_detail_field(acct_id, "Full Name", name)
                update_customer_detail_field(acct_id, "Email Address", email)

        # 4. Add users to Org
        add_users_to_org(org_id, account_ids)

    except Exception as e:
        print(f"❌ Error processing {acc.get('Name')}: {e}")

# ===========================
# LAMBDA HANDLER
# ===========================
def lambda_handler(event, context):
    try:
        token, instance = get_salesforce_token()
        accounts = get_recent_accounts(token, instance)
        print(f"Processing {len(accounts)} updated accounts today.")

        for acc in accounts:
            process_single_account(acc, token, instance)

        return {"status": "ok", "accounts_processed": len(accounts)}

    except Exception as e:
        print(f"❌ Critical Error: {str(e)}")
        return {"status": "error", "message": str(e)}
