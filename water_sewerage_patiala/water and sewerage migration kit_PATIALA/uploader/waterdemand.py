import requests
from dbconfig import *
import math
from datetime import datetime, timedelta
import uuid
from dateutil.relativedelta import relativedelta
import json

def generate_uuid():
    return str(uuid.uuid4())


def get_mdms_tax_periods(tenant_id="pb"):
    """
    Fetches TaxPeriods from MDMS for BillingService -> TaxPeriod.
    Filters only QUARTERLY WS periods.
    """
    url = implementation_url + "/egov-mdms-service/v1/_search"
    headers = {"Content-Type": "application/json"}
    access_token = superuser_login()["access_token"]

    request_body = {
        "RequestInfo": {
            "apiId": "Rainmaker",
            "ver": "1.0",
            "ts": None,
            "action": "_search",
            "did": "1",
            "key": "",
            "msgId": "taxperiod-fetch",
            "authToken": access_token
        },
        "MdmsCriteria": {
            "tenantId": tenant_id,
            "moduleDetails": [
                {
                    "moduleName": "BillingService",
                    "masterDetails": [{"name": "TaxPeriod"}]
                }
            ]
        }
    }

    resp = requests.post(url, headers=headers, data=json.dumps(request_body))
    if resp.status_code not in (200, 201):
        print(f"❌ Error fetching tax periods: {resp.status_code}, {resp.text}")
        return []

    data = resp.json()
    tax_periods = data.get("MdmsRes", {}).get("BillingService", {}).get("TaxPeriod", [])

    # ✅ filter only WS + QUARTERLY
    ws_quarters = [
        tp for tp in tax_periods
        if tp.get("service") == "WS" and tp.get("periodCycle") == "QUATERLY"
    ]
    return ws_quarters


class waterdemand:
    def __init__(self, response, waterData):
        self.response = response
        self.waterData = waterData
        self.tax_periods = get_mdms_tax_periods(response['WaterConnection'][0]['tenantId'])
        self.demandforwater()

    def filter_relevant_tax_periods(self, execution_date, billed_upto_date):
        """
        From MDMS TaxPeriods, select all WS Quarterly periods that fall
        between execution_date and billed_upto_date (inclusive).
        """
        relevant = []
        for tp in self.tax_periods:
            start_epoch = tp["fromDate"]
            end_epoch = tp["toDate"]

            start_dt = datetime.utcfromtimestamp(start_epoch / 1000)
            end_dt = datetime.utcfromtimestamp(end_epoch / 1000)

            # ✅ include any tax period that overlaps with the range
            if start_dt >= execution_date and end_dt <= billed_upto_date:
                relevant.append((start_epoch, end_epoch))

        relevant.sort(key=lambda x: x[0])
        return relevant

    def demandforwater(self):
        pkwsid = self.waterData.get('pkwsid')
        id_old = self.waterData.get('id_old')
        oldconnectionno = self.waterData.get('oldconnectionno')
        consumercode = id_old or oldconnectionno

        print(f"Creating Demand For Water Connection: {pkwsid} (id_old={consumercode})")

        # --- Fetch execution date ---
        execution_date = None
        if consumercode:
            execution_date_query = f"SELECT recpt_dt FROM conn_date WHERE id_old = '{consumercode}' and conn='W'"
            cursor_ws.execute(execution_date_query)
            execution_date_data = cursor_ws.fetchone()
            if execution_date_data:
                execution_date = datetime.strptime(execution_date_data[0], "%d-%m-%Y")

        # --- Fetch arrear data ---
        arrear_query = f"SELECT * FROM arrear_pending WHERE id_no = '{consumercode}'"
        cursor_ws.execute(arrear_query)
        arrear_data = cursor_ws.fetchone()
        if not arrear_data:
            print(f"No arrear data found for {consumercode}")
            return

        colnames = [desc[0] for desc in cursor_ws.description]
        json_arrear = dict(zip(colnames, arrear_data))

        billed_upto_date = datetime.strptime(json_arrear["billed_upto"], "%d/%m/%Y")
        previous_water = float(json_arrear.get("previous_water", 0) or 0)
        current_water = float(json_arrear.get("current_water", 0) or 0)
        surcharge = float(json_arrear.get("surcharge", 0) or 0)

        # ✅ Get MDMS aligned periods
        quarter_periods = self.filter_relevant_tax_periods(execution_date, billed_upto_date)
        if not quarter_periods:
            print(f"No TaxPeriods found between {execution_date} and {billed_upto_date} for {consumercode}")
            return

        selectquery = f"SELECT row_to_json(cd) FROM patiala_migrate_records_uat AS cd WHERE cd.pkwsid = '{pkwsid}' AND cd.isconmig = 'True'"
        cursor_ws.execute(selectquery)
        data = cursor_ws.fetchall()

        if not data:
            print(f"No migration record found for {pkwsid}")
            return

        for row in data:
            json_data = row[0]
            isdemanduploaded = json_data["iswaterdemanduploaded"]
            connno = json_data["pkwsid"]
            payer = self.response['WaterConnection'][0]['connectionHolders'][0]['uuid']

            demands = []
            remaining_advance = previous_water if previous_water < 0 else 0

            for i, (start_epoch, end_epoch) in enumerate(quarter_periods):
                demand_id = generate_uuid()
                demand_details = []
                ws_charge = 0  # Always initialize

                # --- Second last quarter ---
                if i == len(quarter_periods) - 2:
                    if previous_water >= 0:
                        # Case 1: advance is zero → just one WS_CHARGE
                        ws_charge = previous_water
                        demand_details.append({
                            "id": None,
                            "demandId": demand_id,
                            "taxHeadMasterCode": "WS_CHARGE",
                            "taxAmount": previous_water,
                            "collectionAmount": 0,
                            "auditDetails": None,
                            "tenantId": self.response['WaterConnection'][0]['tenantId']
                        })
                    else:
                        # Case 2: advance exists → two rows
                        ws_charge = 0
                        demand_details.append({
                            "id": None,
                            "demandId": demand_id,
                            "taxHeadMasterCode": "WS_CHARGE",
                            "taxAmount": 0,
                            "collectionAmount": 0,
                            "auditDetails": None,
                            "tenantId": self.response['WaterConnection'][0]['tenantId']
                        })
                        demand_details.append({
                            "id": None,
                            "demandId": demand_id,
                            "taxHeadMasterCode": "WS_ADVANCE_CARRYFORWARD",
                            "taxAmount": remaining_advance,  # negative
                            "collectionAmount": remaining_advance,  # also negative
                            "auditDetails": None,
                            "tenantId": self.response['WaterConnection'][0]['tenantId']
                        })

                # --- Last quarter ---
                elif i == len(quarter_periods) - 1:
                    tentative_charge = current_water + surcharge

                    if previous_water >= 0:
                        # Case 1: no advance → only one row
                        ws_charge = tentative_charge
                        demand_details.append({
                            "id": None,
                            "demandId": demand_id,
                            "taxHeadMasterCode": "WS_CHARGE",
                            "taxAmount": tentative_charge,
                            "collectionAmount": 0,
                            "auditDetails": None,
                            "tenantId": self.response['WaterConnection'][0]['tenantId']
                        })
                    else:
                        # Case 2: advance exists → two rows
                        ws_charge = tentative_charge
                        demand_details.append({
                            "id": None,
                            "demandId": demand_id,
                            "taxHeadMasterCode": "WS_CHARGE",
                            "taxAmount": tentative_charge,
                            "collectionAmount": tentative_charge,  # collected same as current
                            "auditDetails": None,
                            "tenantId": self.response['WaterConnection'][0]['tenantId']
                        })
                        demand_details.append({
                            "id": None,
                            "demandId": demand_id,
                            "taxHeadMasterCode": "WS_ADVANCE_CARRYFORWARD",
                            "taxAmount": remaining_advance + tentative_charge,  # advance carry forward
                            "collectionAmount": 0,
                            "auditDetails": None,
                            "tenantId": self.response['WaterConnection'][0]['tenantId']
                        })

                # --- Other quarters ---
                else:
                    ws_charge = 0
                    demand_details.append({
                        "id": None,
                        "demandId": demand_id,
                        "taxHeadMasterCode": "WS_CHARGE",
                        "taxAmount": 0,
                        "collectionAmount": 0,
                        "auditDetails": None,
                        "tenantId": self.response['WaterConnection'][0]['tenantId']
                    })

                # --- Build demand ---
                demand = {
                    "isPaymentCompleted": False,
                    "id": demand_id,
                    "tenantId": self.response['WaterConnection'][0]['tenantId'],
                    "consumerCode": self.response['WaterConnection'][0]['connectionNo'],
                    "consumerType": "waterConnection",
                    "businessService": "WS",
                    "payer": {
                        "id": None,
                        "userName": self.response['WaterConnection'][0]['connectionHolders'][0]['userName'],
                        "name": self.response['WaterConnection'][0]['connectionHolders'][0]['name'],
                        "type": "CITIZEN",
                        "mobileNumber": self.response['WaterConnection'][0]['connectionHolders'][0]['mobileNumber'],
                        "emailId": None,
                        "roles": [{
                            "id": None,
                            "name": "Citizen",
                            "code": "CITIZEN",
                            "tenantId": "pb"
                        }],
                        "tenantId": "pb",
                        "uuid": self.response['WaterConnection'][0]['connectionHolders'][0]['uuid']  # ✅ FIXED
                    },
                    "taxPeriodFrom": start_epoch,
                    "taxPeriodTo": end_epoch,
                    "demandDetails": demand_details,
                    "auditDetails": None,
                    "billExpiryTime": 2592000000,
                    "additionalDetails": None,
                    "minimumAmountPayable": ws_charge if ws_charge > 0 else 0,
                    "status": "ACTIVE"
                }
                demands.append(demand)

            # --- Create Demand API ---
            url = implementation_url + "/billing-service/demand/_create"
            headers = {"Content-Type": "application/json"}
            access_token = superuser_login()["access_token"]

            request_body = {
                "RequestInfo": {
                    "apiId": "Rainmaker",
                    "ver": "abc",
                    "ts": None,
                    "action": "_jobscheduler",
                    "did": "1",
                    "key": "0107000078",
                    "msgId": "pb.patiala",
                    "authToken": access_token,
                    "correlationId": None,
                    "userInfo": {
                        "id": 1129330,
                        "userName": "30048|1kLQtFI+5IIm6SFcjIfhZ0tP0zMutTQ=",
                        "name": "30048|xmnsjn0chhlSQakCdJnN1JDAsabIObUqPfdgESHoCA==",
                        "type": "SYSTEM",
                        "mobileNumber": "30048|rCGmyyFAn0sZH/QG9YS9JwjDyvkGl40MWUE=",
                        "emailId": None,
                        "roles": [
                            {"id": None, "name": "Employee", "code": "EMPLOYEE", "tenantId": "pb"},
                            {"id": None, "name": "SYSTEM user for cron job", "code": "SYSTEM", "tenantId": "pb"}
                        ],
                        "tenantId": "pb.patiala",
                        "uuid": "cdbba3bc-5d4f-4742-bbb7-418d8dfce7fd"
                    }
                },
                "Demands": demands
            }

            request_body_string = json.dumps(request_body)

            if isdemanduploaded == 'False':
                demand_response = requests.post(url, headers=headers, data=json.dumps(request_body))
                response_decoded = demand_response.content.decode('utf-8')

                if demand_response.status_code in (201, 200):
                    print(f"Demand created successfully for {connno}")
                    try:
                        response_json = json.loads(response_decoded)
                        new_demand_id = response_json["Demands"][0]["id"]
                        audit_info = response_json["Demands"][0]["auditDetails"]

                        created_by = audit_info.get("createdBy")
                        last_modified_by = audit_info.get("lastModifiedBy")
                        created_time = audit_info.get("createdTime")
                        last_modified_time = audit_info.get("lastModifiedTime")

                        # ✅ Insert WS_ADVANCE_CARRYFORWARD rows if present
                        for dd in response_json["Demands"][0]["demandDetails"]:
                            if dd["taxHeadMasterCode"] == "WS_ADVANCE_CARRYFORWARD":
                                advance_uuid = generate_uuid()
                                insert_query = f"""
                                INSERT INTO egbs_demanddetail_v1 (
                                    id, demandid, taxheadcode, taxamount, collectionamount,
                                    createdby, createdtime, lastmodifiedby, lastmodifiedtime, tenantid, additionaldetails
                                ) VALUES (
                                    '{advance_uuid}', 
                                    '{new_demand_id}', 
                                    'WS_ADVANCE_CARRYFORWARD', 
                                    {dd['taxAmount']}, 
                                    0.00, 
                                    '{created_by}', 
                                    '{created_time}', 
                                    '{last_modified_by}', 
                                    '{last_modified_time}', 
                                    '{self.response['WaterConnection'][0]['tenantId']}', 
                                    NULL
                                );
                                """
                                update_query = """
                                    UPDATE patiala_migrate_records_uat 
                                    SET addvance_query = %s 
                                    WHERE pkwsid = %s;
                                """
                                cursor_ws.execute(update_query, (insert_query, connno))
                                connection_db_ws.commit()
                                print(f"Advance INSERT query saved for {connno}")

                    except Exception as e:
                        print(f"Error processing response for {connno}: {str(e)}")

                    update_query = """
                        UPDATE patiala_migrate_records_uat 
                        SET water_upload_status = 'COMPLETED', 
                            water_upload_req = %s, 
                            water_upload_res = %s, 
                            iswaterdemanduploaded = 'True' 
                        WHERE pkwsid = %s;
                    """
                    cursor_ws.execute(update_query, (request_body_string, response_decoded, connno))

                    update_watertable = """
                        UPDATE patiala_migrate_records_uat 
                        SET iswaterdemandmigrated = 'True' 
                        WHERE pkwsid = %s;
                    """
                    cursor_ws.execute(update_watertable, (connno,))
                    connection_db_ws.commit()

                elif demand_response.status_code == 400:
                    print(f"Demand already exists for {connno}")
                    update_query = """
                        UPDATE patiala_migrate_records_uat 
                        SET water_upload_status = 'COMPLETED', 
                            water_upload_req = %s, 
                            water_upload_res = %s, 
                            iswaterdemanduploaded = 'True' 
                        WHERE pkwsid = %s;
                    """
                    cursor_ws.execute(update_query, (request_body_string, response_decoded, connno))
                    update_watertable = """
                        UPDATE patiala_migrate_records_uat 
                        SET iswaterdemandmigrated = 'True' 
                        WHERE pkwsid = %s;
                    """
                    cursor_ws.execute(update_watertable, (connno,))
                    connection_db_ws.commit()

                else:
                    print(f"Error creating demand for {connno}: {demand_response.status_code}")
                    update_query = """
                        UPDATE patiala_migrate_records_uat 
                        SET water_upload_status = 'ERROR', 
                            water_upload_req = %s, 
                            iswaterdemanduploaded = 'False' 
                        WHERE pkwsid = %s;
                    """
                    cursor_ws.execute(update_query, (request_body_string, connno))
                    connection_db_ws.commit()


connection_db_ws.commit()
