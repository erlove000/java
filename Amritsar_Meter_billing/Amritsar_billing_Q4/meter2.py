import requests
import json
import psycopg2
import os
import requests
import datetime
import certifi
import time
def handle_meter_status():
    date_object = datetime.datetime.strptime("01-10-2024", "%d-%m-%Y")
    prev_date = date_object.strftime("%d/%m/%Y")
    date_objects = datetime.datetime.strptime("31-12-2024", "%d-%m-%Y")
    current_date = date_objects.strftime("%d/%m/%Y")
    billingPeriod = prev_date + "-" + current_date

    prev_date_epoch = datetime.datetime.strptime("01-10-2024", "%d-%m-%Y")
    prev_date_epoch = int(prev_date_epoch.timestamp() * 1000)
    current_date_epoch = datetime.datetime.strptime("31-12-2024", "%d-%m-%Y")
    current_date_epoch = int(current_date_epoch.timestamp() * 1000)

    # Create the API payload with database values
    payload = {
        "RequestInfo": {
            "apiId": "Rainmaker",
            "ver": ".01",
            "action": "",
            "did": "1",
            "key": "",
            "msgId": "20170310130900|en_IN",
            "requesterId": "",
            "authToken": "4839db6b-71ea-48d0-9f45-6de61ac4463a",
            "userInfo": {
                "id": 3288460,
                "userName": "EMP-AMRITSAR-MIGRATOR",
                "salutation": None,
                "name": "EMP-AMRITSAR-MIGRATOR",
                "gender": "MALE",
                "mobileNumber": "9459286077",
                "emailId": None,
                "altContactNumber": None,
                "pan": None,
                "aadhaarNumber": None,
                "permanentAddress": None,
                "permanentCity": None,
                "permanentPinCode": None,
                "correspondenceAddress": None,
                "correspondenceCity": None,
                "correspondencePinCode": None,
                "alternatemobilenumber": None,
                "active": True,
                "locale": None,
                "type": "EMPLOYEE",
                "accountLocked": False,
                "accountLockedDate": 0,
                "fatherOrHusbandName": "Test",
                "relationship": None,
                "signature": None,
                "bloodGroup": None,
                "photo": None,
                "identificationMark": None,
                "createdBy": 10981,
                "lastModifiedBy": 1,
                "tenantId": "pb.amritsar",
                "roles": [
                    {"code": "WS_DOC_VERIFIER", "name": "WS Document Verifier", "tenantId": "pb.amritsar"},
                    {"code": "SW_DOC_VERIFIER", "name": "SW Document Verifier", "tenantId": "pb.amritsar"}
                ],
                "uuid": "e596bfcc-9331-4306-af2b-d1162b463263",
                "createdDate": "17-11-2023 12:55:24",
                "lastModifiedDate": "20-06-2024 22:56:32",
                "dob": "12/2/1982",
                "pwdExpiryDate": "15-02-2024 12:55:24"
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

    # Convert payload to JSON string
    # payload_json = json.dumps(payload, indent=4)

    # Set the API request headers
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer ZWdvdi11c2VyLWNsaWVudDplZ292LXVzZXItc2VjcmV0',
    }

    payload_json = json.dumps(payload)
    print("Request Payload For Connectionno: " + payload_json)

    # response = requests.post(API_ENDPOINT,headers=headers, json=json_payload)
    response = requests.post(API_ENDPOINT, data=payload_json, headers=headers)
    print("Response Connectionno: " + str(response.text))

    print("Response status code:", response.status_code)
    print("Response body:", response.text)
    timestamp = time.time()
    local_time = time.localtime(timestamp)
    now = time.strftime("%Y-%m-%d %H:%M:%S", local_time)
    if response.status_code == 200:

        print(f"API request successful for connection {json_data['id_no']}!")
        # Update the table with the API response status
        cur.execute("UPDATE meter_billing SET response_code = '200', ismigrationdate ='" + str(
            now) + "',response='" + str(response.text) + "' WHERE id_no = %s", (json_data['id_no'],))
        connection_db_ws.commit()
    #elif response.status_code == 400 and str(response.text)._contains_("Billing Period Already Exists"):
    #elif response.status_code == 400 and "Billing Period Already Exists" in str(response.text):

        cur.execute(
            "UPDATE meter_billing SET response_code = '" + str(
                response.status_code) + "', ismigrationdate ='" + str(
                now) + "', response='Billing Period Already Exists' WHERE id_no = %s", (json_data['id_no'],))
        connection_db_ws.commit()
    else:
        print(f"API request failed for connection {json_data['id_no']}:", response.text)
        cur.execute("UPDATE meter_billing SET response_code = '" + str(
            response.status_code) + "', ismigrationdate ='" + str(now) + "', response='" + str(
            response.text) + "' WHERE id_no = %s", (json_data['id_no'],))
        connection_db_ws.commit()

    print("---------------------------------------------------------------------------------------------")



# Database connection settings
dbname = os.getenv("DB_NAME", "MeterQ3Data")
dbuser = os.getenv("DB_USER", "postgres")
dbpassword = os.getenv("DB_PASSWORD", "postgres")
host = os.getenv("DB_HOST", "localhost")

# API endpoint and authentication token
API_ENDPOINT = 'http://localhost:1001/ws-calculator/meterConnection/_create?'
AUTH_TOKEN = '2839bf93-e46d-4b63-98a0-d9fb1928a490'

connection_db_ws = psycopg2.connect("dbname={} user={} password={} host={}".format(dbname, dbuser, dbpassword, host))
cur = connection_db_ws.cursor()

# Query the database to retrieve the values
#cur.execute("SELECT  row_to_json(cd)  FROM meter_reading_entry as cd where response is null AND NOT (isdead = 'TRUE' AND islocked = 'TRUE');")
#cur.execute("SELECT row_to_json(cd) FROM meter_billing_missing AS cd where response_code='400';")
#cur.execute("SELECT row_to_json(cd) FROM meter_billing AS cd where id_no='197554';")
#cur.execute("SELECT row_to_json(cd) FROM meter_billing AS cd where isdead='TRUE' and islocked='FALSE' and prev_rdg is not null and response_code is null and prev_rdg = '0.0' limit 500 offset 0;")
cur.execute("""SELECT row_to_json(cd) FROM meter_billing_missing AS cd where id_no in ('11071')
;""")


#cur.execute("""
#   SELECT row_to_json(cd)
 #   FROM meter_reading_entry AS cd
  #  WHERE response IS NULL
   #   AND isdead = 'TRUE'
    #  AND islocked = 'FALSE';
#""")

data_row = cur.fetchmany(6000)
# Fetch all rows

# Iterate over the rows
for row in data_row:

    json_data = row[0]
    print("Meter Reading Entry For Connectionno: " + str(json_data['id_no']))
    if json_data["isdead"] == "True" or json_data["isdead"] == "TRUE":
        status = "Breakdown"
        previous_meter_reading = json_data.get("prev_rdg")  # Retrieve the previous reading
        current_meter_reading = previous_meter_reading  # Set to the previous reading
        handle_meter_status()



    if json_data["islocked"] == "True" or json_data["islocked"] == "TRUE":
        status = "Locked"
        previous_meter_reading = json_data.get("prev_rdg")  # Retrieve the previous reading
        current_meter_reading = previous_meter_reading  # Set to the previous reading
        handle_meter_status()


    if json_data["prev_rdg"] == "N":
        status = "No-meter"
        previous_meter_reading = 0
        current_meter_reading = 0

    if json_data["islocked"] == "FALSE" and json_data["isdead"] == "FALSE":
        status = "Working"
        previous_meter_reading = json_data["prev_rdg"]
        current_meter_reading = json_data["curr_rdg"]
        handle_meter_status()

    # if status=="Working":
    #     date_object = datetime.datetime.strptime("01-10-2024", "%d-%m-%Y")
    #     prev_date = date_object.strftime("%d/%m/%Y")
    #     date_objects = datetime.datetime.strptime("31-12-2024", "%d-%m-%Y")
    #     current_date = date_objects.strftime("%d/%m/%Y")
    #     billingPeriod = prev_date + "-" + current_date
    #
    #     prev_date_epoch = datetime.datetime.strptime("01-10-2024", "%d-%m-%Y")
    #     prev_date_epoch = int(prev_date_epoch.timestamp() * 1000)
    #     current_date_epoch = datetime.datetime.strptime("31-12-2024", "%d-%m-%Y")
    #     current_date_epoch = int(current_date_epoch.timestamp() * 1000)
    # else:
    #     date_object = datetime.datetime.strptime("01-10-2024", "%d-%m-%Y")
    #     prev_date = date_object.strftime("%d/%m/%Y")
    #     date_objects = datetime.datetime.strptime("31-12-2024", "%d-%m-%Y")
    #     current_date = date_objects.strftime("%d/%m/%Y")
    #     billingPeriod = prev_date + "-" + current_date
    #
    #     prev_date_epoch = datetime.datetime.strptime("01-10-2024", "%d-%m-%Y")
    #     prev_date_epoch = int(prev_date_epoch.timestamp() * 1000)
    #     current_date_epoch = datetime.datetime.strptime("31-12-2024", "%d-%m-%Y")
    #     current_date_epoch = int(current_date_epoch.timestamp() * 1000)


    # Create the API payload with database values
    # payload = {
    #     "RequestInfo": {
    #         "apiId": "Rainmaker",
    #         "ver": ".01",
    #         "action": "",
    #         "did": "1",
    #         "key": "",
    #         "msgId": "20170310130900|en_IN",
    #         "requesterId": "",
    #         "authToken": "5704991f-24c4-4929-a308-91a9d1433634",
    #         "userInfo": {
    #             "id": 3288460,
    #             "userName": "EMP-AMRITSAR-MIGRATOR",
    #             "salutation": None,
    #             "name": "EMP-AMRITSAR-MIGRATOR",
    #             "gender": "MALE",
    #             "mobileNumber": "9459286077",
    #             "emailId": None,
    #             "altContactNumber": None,
    #             "pan": None,
    #             "aadhaarNumber": None,
    #             "permanentAddress": None,
    #             "permanentCity": None,
    #             "permanentPinCode": None,
    #             "correspondenceAddress": None,
    #             "correspondenceCity": None,
    #             "correspondencePinCode": None,
    #             "alternatemobilenumber": None,
    #             "active": True,
    #             "locale": None,
    #             "type": "EMPLOYEE",
    #             "accountLocked": False,
    #             "accountLockedDate": 0,
    #             "fatherOrHusbandName": "Test",
    #             "relationship": None,
    #             "signature": None,
    #             "bloodGroup": None,
    #             "photo": None,
    #             "identificationMark": None,
    #             "createdBy": 10981,
    #             "lastModifiedBy": 1,
    #             "tenantId": "pb.amritsar",
    #             "roles": [
    #                 {"code": "WS_DOC_VERIFIER", "name": "WS Document Verifier", "tenantId": "pb.amritsar"},
    #                 {"code": "SW_DOC_VERIFIER", "name": "SW Document Verifier", "tenantId": "pb.amritsar"}
    #             ],
    #             "uuid": "e596bfcc-9331-4306-af2b-d1162b463263",
    #             "createdDate": "17-11-2023 12:55:24",
    #             "lastModifiedDate": "20-06-2024 22:56:32",
    #             "dob": "12/2/1982",
    #             "pwdExpiryDate": "15-02-2024 12:55:24"
    #         }
    #     },
    #     "meterReadings": {
    #         "currentReadingDate": current_date_epoch,
    #         "currentReading": current_meter_reading,
    #         "billingPeriod": billingPeriod,
    #         "meterStatus": status,
    #         "connectionNo": json_data['id_no'],
    #         "lastReading": previous_meter_reading,
    #         "lastReadingDate": prev_date_epoch,
    #         "tenantId": "pb.amritsar",
    #         "generateDemand": True
    #     }
    # }
    #
    # # Convert payload to JSON string
    # # payload_json = json.dumps(payload, indent=4)
    #
    # # Set the API request headers
    # headers = {
    #     'Content-Type': 'application/json',
    #     'Authorization': f'Bearer ZWdvdi11c2VyLWNsaWVudDplZ292LXVzZXItc2VjcmV0',
    # }
    #
    # payload_json = json.dumps(payload)
    # print("Request Payload For Connectionno: " + payload_json)
    #
    # # response = requests.post(API_ENDPOINT,headers=headers, json=json_payload)
    # response = requests.post(API_ENDPOINT, data=payload_json, headers=headers)
    # print("Response Connectionno: " + str(response.text))
    #
    # print("Response status code:", response.status_code)
    # print("Response body:", response.text)
    # timestamp = time.time()
    # local_time = time.localtime(timestamp)
    # now = time.strftime("%Y-%m-%d %H:%M:%S", local_time)
    # if response.status_code == 200:
    #
    #     print(f"API request successful for connection {json_data['id_no']}!")
    #     # Update the table with the API response status
    #     cur.execute("UPDATE north_billing SET response_code = '200', ismigrationdate ='" + str(
    #         now) + "',response='" + str(response.text) + "' WHERE id_no = %s", (json_data['id_no'],))
    #     connection_db_ws.commit()
    # #elif response.status_code == 400 and str(response.text)._contains_("Billing Period Already Exists"):
    #     #cur.execute(
    #    #     "UPDATE north_billing SET response_code = '" + str(
    #            # response.status_code) + "', ismigrationdate ='" + str(
    #           #  now) + "', response='Billing Period Already Exists' WHERE id_no = %s", (json_data['id_no'],))
    #     #connection_db_ws.commit()
    # else:
    #     print(f"API request failed for connection {json_data['id_no']}:", response.text)
    #     cur.execute("UPDATE north_billing SET response_code = '" + str(
    #         response.status_code) + "', ismigrationdate ='" + str(now) + "', response='" + str(
    #         response.text) + "' WHERE id_no = %s", (json_data['id_no'],))
        connection_db_ws.commit()

    print("---------------------------------------------------------------------------------------------")
    # Close the database connection


cur.close()
connection_db_ws.close()