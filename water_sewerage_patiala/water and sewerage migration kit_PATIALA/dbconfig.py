import psycopg2
import json
import os
import json
import uuid
from urllib.parse import urljoin

import numpy
import pandas as pd
import re
import requests



implementation_url="https://sdc-uat.lgpunjab.gov.in"


#-------------------------- Empployee Login Detail------------------------

Employee_username="EMP-PATIALA-MIGRATOR"
# Employee_password="Block$1234@"
Employee_password="eGov@123"
Employee_tenatid="pb.patiala"



def superuser_login():
    return login_egov(Employee_username, Employee_password, Employee_tenatid, "EMPLOYEE")


def login_egov(username, password, tenant_id, user_type="EMPLOYEE"):
    url = "https://sdc-uat.lgpunjab.gov.in/user/oauth/token"

    headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'en-US,en;q=0.9',
        'authorization': 'Basic ZWdvdi11c2VyLWNsaWVudDplZ292LXVzZXItc2VjcmV0',
        'content-type': 'application/x-www-form-urlencoded',
        'cookie': '_ga=GA1.1.1566922505.1705645335; _ga_6H4CC40238=GS1.1.1734081558.25.0.1734082755.0.0.0; pga4_session=8c134dea-c63d-46c7-a70f-2715c5444c9a!q38eNY8QjJqadpBdEjJydgbgvoKbruLnIWVRA3P7SfQ=; PGADMIN_LANGUAGE=en',
        'origin': 'https://sdc-uat.lgpunjab.gov.in',
        'priority': 'u=1, i',
        'referer': 'https://sdc-uat.lgpunjab.gov.in/employee/user/login',
        'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'
    }

    data = {
        'username': username,
        'password': password,
        'grant_type': 'password',
        'scope': 'read',
        'tenantId': tenant_id,
        'userType': user_type
    }

    # Send the POST request
    resp = requests.post(url, headers=headers, data=data)

    # Check the status code
    if resp.status_code != 200:
        # Log or print the error and response content for debugging
        print(f"Error: Status Code {resp.status_code}")
        print(f"Response: {resp.text}")
        raise Exception(f"Login failed with status code {resp.status_code}")

    try:
        # Attempt to parse the response as JSON
        return resp.json()
    except json.decoder.JSONDecodeError:
        # Handle cases where the response body is not valid JSON
        print(f"Failed to parse JSON response: {resp.text}")
        raise Exception("Invalid JSON response from the server.")
#----------------------------Data base Credentials ------------------------------------------#
dbname = os.getenv("DB_NAME", "Patiala_Migration")
dbuser = os.getenv("DB_USER", "postgres")
dbpassword = os.getenv("DB_PASSWORD", "postgres")

host = os.getenv("DB_HOST", "localhost")
batch = os.getenv("BATCH_NAME", "2")
table_name = os.getenv("TABLE_NAME", "patiala_migrate_records_uat")

default_locality = os.getenv("DEFAULT_LOCALITY", "UNKNOWN")
batch_size = os.getenv("BATCH_SIZE", "100")

dry_run = True

#connection_db_pt = psycopg2.connect("dbname={} user={} password={} host={}".format(dbname, dbuser, dbpassword, host))
connection_db_ws = psycopg2.connect("dbname={} user={} password={} host={}".format(dbname, dbuser, dbpassword, host))
cursor_ws = connection_db_ws.cursor()

