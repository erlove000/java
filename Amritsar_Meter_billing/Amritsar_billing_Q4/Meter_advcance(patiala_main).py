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
AUTH_TOKEN = 'e3ddfbfa-3d8a-4ac0-943b-4cbb53b0c465'
SEARCH_API_TOKEN = 'e3ddfbfa-3d8a-4ac0-943b-4cbb53b0c465'

# Database config
dbname = os.getenv("DB_NAME", "PatialaData")
dbuser = os.getenv("DB_USER", "postgres")
dbpassword = os.getenv("DB_PASSWORD", "postgres")
host = os.getenv("DB_HOST", "localhost")

connection_db_ws = psycopg2.connect(
    dbname=dbname, user=dbuser, password=dbpassword, host=host
)
cur = connection_db_ws.cursor()


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
                "userName": "EMP-PATIALA-MIGRATOR",
                "name": "EMP-PATIALA-MIGRATOR",
                "gender": "MALE",
                "mobileNumber": "9459286077",
                "active": True,
                "type": "EMPLOYEE",
                "tenantId": "pb.patiala",
                "roles": [
                    {"code": "WS_DOC_VERIFIER", "name": "WS Document Verifier", "tenantId": "pb.patiala"},
                    {"code": "SW_DOC_VERIFIER", "name": "SW Document Verifier", "tenantId": "pb.patiala"}
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
            "tenantId": "pb.patiala",
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
            UPDATE patiala_billing_q4
            SET response_code = '200', ismigrationdate = %s, response = %s
            WHERE id_no = %s
        """, (now, response.text, json_data['id_no']))
    else:
        print(f"API request failed for connection {json_data['id_no']}: {response.text}")
        cur.execute("""
            UPDATE patiala_billing_q4
            SET response_code = %s, ismigrationdate = %s, response = %s
            WHERE id_no = %s
        """, (str(response.status_code), now, response.text, json_data['id_no']))
    connection_db_ws.commit()
    print("---------------------------------------------------------------------------------------------")


# Fetch data from DB
#cur.execute("SELECT  row_to_json(cd)  FROM meter_billing as cd where response is null AND isdead = 'FALSE' AND islocked = 'TRUE' and prev_rdg is not null limit 1000 OFFSET 0;")
#cur.execute("SELECT  row_to_json(cd)  FROM meter_billing as cd where response is null AND isdead = 'FALSE' AND islocked = 'FALSE' limit 1000 OFFSET 0;")
#cur.execute("SELECT row_to_json(cd) FROM meter_billing AS cd WHERE id_no = '12519';")
cur.execute("SELECT row_to_json(cd) FROM patiala_billing_q4 AS cd WHERE id_no in ('1910W000232','1910W000585','1910W000186','1910W000633','1910W000356','1910W000156','1910W000204','1910W000128','1910W000013','1910W000331','1910W000136','1910W000034','1910W000609') and response_code is null;")
#cur.execute("SELECT  row_to_json(cd)  FROM meter_billing as cd where response_code='null' limit 250 OFFSET 0;")

data_row = cur.fetchmany(6000)

for row in data_row:
    json_data = row[0]
    connection_no = json_data['id_no']
    print("Processing Connectionno:", connection_no)

    # Meter status logic
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
