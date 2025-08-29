import requests
import json
import psycopg2
import os
import datetime
import time

# Set the billing period to process
date_object = datetime.datetime.strptime("01-01-2025", "%d-%m-%Y")
prev_date = date_object.strftime("%d/%m/%Y")
date_objects = datetime.datetime.strptime("31-03-2025", "%d-%m-%Y")
current_date = date_objects.strftime("%d/%m/%Y")
billingPeriod = prev_date + "-" + current_date

prev_date_epoch = int(date_object.timestamp() * 1000)
current_date_epoch = int(date_objects.timestamp() * 1000)

# API endpoint and tokens
API_ENDPOINT = 'http://localhost:1001/ws-calculator/meterConnection/_create?'
AUTH_TOKEN = '8edcd4a6-614f-4c49-a00c-cda1a5b62788'
SEARCH_API_TOKEN = '8edcd4a6-614f-4c49-a00c-cda1a5b62788'

# Database config
dbname = os.getenv("DB_NAME", "MeterQ3Data")
dbuser = os.getenv("DB_USER", "postgres")
dbpassword = os.getenv("DB_PASSWORD", "postgres")
host = os.getenv("DB_HOST", "localhost")

connection_db_ws = psycopg2.connect(
    dbname=dbname, user=dbuser, password=dbpassword, host=host
)
cur = connection_db_ws.cursor()


def should_process_connection(connection_no):
    """Checks if billingPeriod '01/10/2024-31/12/2024' or '30/09/2024-31/12/2024' exists for a connection"""
    url = f"https://mseva.lgpunjab.gov.in/ws-calculator/meterConnection/_search?tenantId=pb.amritsar&connectionNos={connection_no}&offset=0"
    headers = {
        'Content-Type': 'application/json;charset=UTF-8',
        'accept': 'application/json',
    }
    payload = {
        "RequestInfo": {
            "apiId": "Rainmaker",
            "ver": ".01",
            "action": "_search",
            "did": "1",
            "key": "",
            "msgId": "20170310130900|en_IN",
            "requesterId": "",
            "authToken": SEARCH_API_TOKEN
        }
    }

    try:
        res = requests.post(url, headers=headers, json=payload, timeout=15)
        if res.status_code == 200:
            data = res.json()
            readings = data.get("meterReadings", [])
            required_periods = ["01/10/2024-31/12/2024", "30/09/2024-31/12/2024"]

            for r in readings:
                period = r.get("billingPeriod", "").replace(" ", "").strip()
                if period in [p.replace(" ", "") for p in required_periods]:
                    print(f"✅ Found billing period for {connection_no}: {period}")
                    return True

            print(f"❌ Billing period not found for {connection_no}")
            return False
        else:
            print(f"Search API failed for {connection_no}: {res.status_code}")
            return False
    except Exception as e:
        print(f"Error for connection {connection_no}: {e}")
        return False



def handle_meter_status():
    payload = {
        "RequestInfo": {
            "apiId": "Rainmaker",
            "ver": ".01",
            "action": "",
            "did": "1",
            "key": "",
            "msgId": "20170310130900|en_IN",
            "requesterId": "",
            "authToken": AUTH_TOKEN,
            "userInfo": {
                "id": 3288460,
                "userName": "EMP-AMRITSAR-MIGRATOR",
                "name": "EMP-AMRITSAR-MIGRATOR",
                "gender": "MALE",
                "mobileNumber": "9459286077",
                "active": True,
                "type": "EMPLOYEE",
                "tenantId": "pb.amritsar",
                "roles": [
                    {"code": "WS_DOC_VERIFIER", "name": "WS Document Verifier", "tenantId": "pb.amritsar"},
                    {"code": "SW_DOC_VERIFIER", "name": "SW Document Verifier", "tenantId": "pb.amritsar"}
                ],
                "uuid": "e596bfcc-9331-4306-af2b-d1162b463263"
            }
        },
        "meterReadings": {
            "currentReadingDate": current_date_epoch,
            "currentReading": current_meter_reading,
            "billingPeriod": billingPeriod,
            "meterStatus": status,
            "connectionNo": json_data['id_no'],
            "lastReading": previous_meter_reading,
            "lastReadingDate": prev_date_epoch,
            "tenantId": "pb.amritsar",
            "generateDemand": True
        }
    }

    headers = {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer ZWdvdi11c2VyLWNsaWVudDplZ292LXVzZXItc2VjcmV0',
    }

    payload_json = json.dumps(payload)
    print("Request Payload For Connectionno:", payload_json)

    response = requests.post(API_ENDPOINT, data=payload_json, headers=headers)
    print("Response Connectionno:", str(response.text))
    print("Response status code:", response.status_code)

    now = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    if response.status_code == 200:
        print(f"API request successful for connection {json_data['id_no']}!")
        cur.execute("""
            UPDATE meter_billing
            SET response_code = '200', ismigrationdate = %s, response = %s
            WHERE id_no = %s
        """, (now, response.text, json_data['id_no']))
    else:
        print(f"API request failed for connection {json_data['id_no']}: {response.text}")
        cur.execute("""
            UPDATE meter_billing
            SET response_code = %s, ismigrationdate = %s, response = %s
            WHERE id_no = %s
        """, (str(response.status_code), now, response.text, json_data['id_no']))
    connection_db_ws.commit()
    print("---------------------------------------------------------------------------------------------")


# Fetch data from DB
#cur.execute("SELECT  row_to_json(cd)  FROM meter_billing as cd where response is null AND isdead = 'FALSE' AND islocked = 'TRUE' and prev_rdg is not null limit 1000 OFFSET 1000;")
cur.execute("SELECT  row_to_json(cd)  FROM meter_billing as cd where response_code='null' limit 250 OFFSET 250;")
#cur.execute("SELECT row_to_json(cd) FROM meter_billing AS cd WHERE id_no = '155118';")
#cur.execute("SELECT row_to_json(cd) FROM meter_billing AS cd WHERE id_no in ('1982');")

data_row = cur.fetchmany(6000)

for row in data_row:
    json_data = row[0]
    connection_no = json_data['id_no']
    print("Processing Connectionno:", connection_no)

    # Check if required billing period exists
    if not should_process_connection(connection_no):
        print(f"Skipping {connection_no} due to missing required billing period.")

        now = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        cur.execute("""
            UPDATE meter_billing
            SET response_code = '400',
                ismigrationdate = %s,
                response = %s
            WHERE id_no = %s
        """, (now, 'Missing required billing period', connection_no))
        connection_db_ws.commit()

        continue

    # Determine status and call handler
    if json_data["isdead"] in ["True", "TRUE"]:
        status = "Breakdown"
        previous_meter_reading = json_data.get("prev_rdg")
        current_meter_reading = previous_meter_reading
        handle_meter_status()

    elif json_data["islocked"] in ["True", "TRUE"]:
        status = "Locked"
        previous_meter_reading = json_data.get("prev_rdg")
        current_meter_reading = previous_meter_reading
        handle_meter_status()

    elif json_data["prev_rdg"] == "N":
        status = "No-meter"
        previous_meter_reading = 0
        current_meter_reading = 0

    elif json_data["islocked"] == "FALSE" and json_data["isdead"] == "FALSE":
        status = "Working"
        previous_meter_reading = json_data["prev_rdg"]
        current_meter_reading = json_data["curr_rdg"]
        handle_meter_status()

# Cleanup
cur.close()
connection_db_ws.close()
