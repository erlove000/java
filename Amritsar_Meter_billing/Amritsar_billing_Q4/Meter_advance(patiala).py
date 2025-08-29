import requests
import json
import psycopg2
import os
import datetime
import time

# ------------------- CONFIG -------------------
QUARTERS_FOR_BILLING = [
    "30/09/2023", "31/12/2023", "31/03/2024", "30/06/2024",
    "30/09/2024", "31/12/2024", "31/03/2025", "30/06/2025",
    "30/09/2025", "31/12/2025", "31/03/2026"
]

API_ENDPOINT = 'http://localhost:1001/ws-calculator/meterConnection/_create?'
AUTH_TOKEN = 'e3ddfbfa-3d8a-4ac0-943b-4cbb53b0c465'

# ------------------- DB CONNECTION -------------------
dbname = os.getenv("DB_NAME", "PatialaData")
dbuser = os.getenv("DB_USER", "postgres")
dbpassword = os.getenv("DB_PASSWORD", "postgres")
host = os.getenv("DB_HOST", "localhost")

connection_db_ws = psycopg2.connect(
    dbname=dbname, user=dbuser, password=dbpassword, host=host
)
cur = connection_db_ws.cursor()

# ------------------- UTILS -------------------
def get_connection_execution_period(epoch_ms):
    try:
        dt = datetime.datetime.fromtimestamp(int(epoch_ms) / 1000)
        for q_end_str in QUARTERS_FOR_BILLING:
            q_end = datetime.datetime.strptime(q_end_str, "%d/%m/%Y")
            if dt <= q_end:
                return f"{dt.strftime('%d/%m/%Y')} - {q_end.strftime('%d/%m/%Y')}", dt, q_end
        return None, None, None
    except Exception as e:
        print(f"❌ Error converting execution date from epoch {epoch_ms}: {e}")
        return None, None, None

# ------------------- METER STATUS HANDLER -------------------
def handle_meter_status():
    billing_period_str, start_dt, end_dt = get_connection_execution_period(json_data['connectionexecutiondate'])
    if not billing_period_str:
        print(f"⚠️ Skipping {json_data['id_no']} due to invalid connectionexecutiondate.")
        return

    last_reading_epoch = int(start_dt.timestamp() * 1000)
    current_reading_epoch = int(end_dt.timestamp() * 1000)

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
                "uuid": "8127b8d4-8e14-44e8-aa9b-b39577c56650"
            }
        },
        "meterReadings": {
            "currentReadingDate": current_reading_epoch,
            "currentReading": current_meter_reading,
            "billingPeriod": billing_period_str,
            "meterStatus": status,
            "connectionNo": json_data['id_no'],
            "lastReading": previous_meter_reading,
            "lastReadingDate": last_reading_epoch,
            "tenantId": "pb.patiala",
            "generateDemand": True
        }
    }

    headers = {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer ZWdvdi11c2VyLWNsaWVudDplZ292LXVzZXItc2VjcmV0',
    }

    payload_json = json.dumps(payload)
    print("📤 Request Payload for", json_data['id_no'], ":", payload_json)

    response = requests.post(API_ENDPOINT, data=payload_json, headers=headers)
    print("📥 Response:", response.status_code, response.text)

    now = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    if response.status_code == 200:
        cur.execute("""
            UPDATE patiala_billing
            SET response_code = '200', ismigrationdate = %s, response = %s
            WHERE id_no = %s
        """, (now, response.text, json_data['id_no']))
    else:
        cur.execute("""
            UPDATE patiala_billing
            SET response_code = %s, ismigrationdate = %s, response = %s
            WHERE id_no = %s
        """, (str(response.status_code), now, response.text, json_data['id_no']))
    connection_db_ws.commit()
    print("---------------------------------------------------------------------------------------------")

# ------------------- MAIN LOOP -------------------
cur.execute("SELECT row_to_json(cd) FROM patiala_billing AS cd WHERE id_no in ('1910W000288');")
data_row = cur.fetchmany(1000)

for row in data_row:
    json_data = row[0]
    connection_no = json_data['id_no']
    print(f"🔄 Processing Connectionno: {connection_no}")

    if json_data.get("connectionexecutiondate"):
        billing_period_str, _, _ = get_connection_execution_period(json_data["connectionexecutiondate"])
        if billing_period_str:
            cur.execute("""
                UPDATE patiala_billing
                SET connectionexecutionperiod = %s
                WHERE id_no = %s
            """, (billing_period_str, connection_no))
            connection_db_ws.commit()
            print(f"✅ Stored billing period: {billing_period_str}")

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

# ------------------- CLEANUP -------------------
cur.close()
connection_db_ws.close()
