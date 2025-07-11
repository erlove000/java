from dbconfig import *
import uuid
from decimal import Decimal, ROUND_HALF_UP
from datetime import datetime






class WaterConnection:
    def __init__(self, property_reponse, water_data):
        self.property_reponse = property_reponse
        self.water_data = water_data
        self.createwater(self)

    def createwater(self,waterData):
        print("Creating Water Application")
        connType = waterData.water_data['conn_type']
        if (connType == "B"):
            iswaterconn = True
            issewerageconn = True
            isname="Water And Sewerage"
        elif (connType == "WATER"):
            iswaterconn = True
            issewerageconn = False
            isname = "Water"
        url = "https://sdc-uat.lgpunjab.gov.in/ws-services/wc/_create"
        if (waterData.water_data['mobile']==None or waterData.water_data['mobile']==''):
            waterData.water_data['mobile']='9999999999'
        else:
            waterData.water_data['mobile']=waterData.water_data['mobile']
        conn_holder_detail = conn_holder_details()
        conn_holder=conn_holder_detail.ownerdetails(waterData.water_data['ownername'], waterData.water_data['mobile'])
        ConnHolderDetail=json.loads(conn_holder)
        headers = {
            "Content-Type": "application/json",
        }
        request_body = {
            'RequestInfo': {
                'apiId': 'Rainmaker',
                'ver': '.01',
                'action': '_create',
                'did': '1',
                'key': '',
                'msgId': '20170310130900|en_IN',
                'requesterId': '',
                'authToken': superuser_login()["access_token"]
            },
            'WaterConnection': {
                'water': iswaterconn,
                'sewerage': issewerageconn,
                'property':self.property_reponse[0],
                'connectionHolders': ConnHolderDetail,
                'service': isname,
                'roadCuttingArea': None,
                'proposedWaterClosets': None,
                'proposedToilets': None,
                'noOfTaps': None,
                'noOfWaterClosets': None,
                'noOfToilets': None,
                'proposedTaps': None,
                'isMigrated':True,
                'propertyId': self.property_reponse[0]['propertyId'],
                'additionalDetails': {
                    'initialMeterReading': None,
                    'detailsProvidedBy': '',
                    'isexempted': False,
                    'billingType': 'STANDARD',
                    'billingAmount': 0,
                    'ledgerId': None,
                    'avarageMeterReading': None,
                    'meterMake': None,
                    'compositionFee': None,
                    'userCharges': None,
                    'othersFee': None,
                    'unitUsageType': None,
                    'adhocPenalty': None,
                    'adhocPenaltyComment': None,
                    'adhocPenaltyReason': None,
                    'adhocRebate': None,
                    'adhocRebateComment': None,
                    'adhocRebateReason': None,
                    'estimationFileStoreId': None,
                    'sanctionFileStoreId': None,
                    'estimationLetterDate': None,
                    "connectionCategory": "General",
                    'locality': self.property_reponse[0]['address']['locality']['code'],
                    'pkwsid': waterData.water_data['pkwsid'],
                    'block': waterData.water_data['block'],
                    'propertyno': waterData.water_data['propertyno'],
                    'uidno': waterData.water_data['uidno'],
                    'email': waterData.water_data['email'],
                    'remarks': waterData.water_data['remarks'],
                    'tariff_type': waterData.water_data['tariff_type'],
                    'conn_type': waterData.water_data['conn_type'],
                    'disconn_status': waterData.water_data['disconn_status'],
                    'exempted': waterData.water_data['exempted'],
                    'area_in_sqy': waterData.water_data['area_in_sqy'],
                    'oldconnectionno': waterData.water_data.get('oldconnectionno'),
                    'watersubusagetype': waterData.water_data.get('watersubusagetype'),
                    'dischargeconnection': waterData.water_data.get('dischargeconnection'),
                    'ownername': waterData.water_data.get('ownername'),
                    'guardianname': waterData.water_data.get('guardianname'),
                    'category' : waterData.water_data.get('category'),
                    'mobile' : waterData.water_data.get('mobile'),
                    'arrear' : waterData.water_data.get('arrear')
                },
                'tenantId':self.property_reponse[0]['tenantId'],
                'processInstance': {
                    'action': 'INITIATE'
                }
            }
        }
        response = requests.post(url, headers=headers, data=json.dumps(request_body))


        response_decoded= response.content.decode('utf-8')
        response_decoded = json.loads(response_decoded)
        response_decoded = json.dumps(response_decoded, indent=2)
        request_body_string = json.dumps(request_body)
        # uuid =  waterData.waterData['uuid']


        if(response.status_code==200):
            api_response_dict = response.json()
            application_no = api_response_dict['WaterConnection'][0]['applicationNo']

            url = 'https://sdc-uat.lgpunjab.gov.in/ws-services/wc/_search?tenantId=' + self.property_reponse[0][
                'tenantId'] + '&isConnectionSearch=true&applicationNumber=' + application_no
            request_body_search = {
                'RequestInfo': {
                    'apiId': 'Rainmaker',
                    'ver': '.01',
                    'action': '_create',
                    'did': '1',
                    'key': '',
                    'msgId': '20170310130900|en_IN',
                    'requesterId': '',
                    'authToken': superuser_login()["access_token"]
                }
            }
            search_response = requests.post(url, headers=headers, data=json.dumps(request_body_search))
            search_response = search_response.json()
            connectionNumebr = waterData.water_data['pkwsid']
            oldconnectionno = waterData.water_data.get('oldconnectionno', '')
            print("Water Application Create Request: " + request_body_string)
            print("Water Application Created Succesfully with Response: " + response_decoded)

            # Fetch receipt_date from the database based on pkwsid
            pkwsid = waterData.water_data['pkwsid']
            #cursor_ws.execute("SELECT receipt_date FROM nangal_migrate_records_uat WHERE pkwsid = %s", (pkwsid,))
            cursor_ws.execute("SELECT receiptdate FROM nangal_migrate_records_uat WHERE pkwsid = %s AND isconmig IS NULL AND sewerage_query_activate is NULL AND water_query_activate is NULL",(pkwsid,))
            result = cursor_ws.fetchone()

            if result and result[0]:
                receiptdate = result[0]  # This is the string from DB like "1/1/2023"
                print(f"Fetched receipt_date from DB: {receiptdate}")

                if isinstance(receiptdate, str):
                    try:
                        receipt_date = receiptdate.strip().replace("//", "/")
                        receipt_date = datetime.strptime(receipt_date, "%d/%m/%Y")
                        print(f"Parsed receipt_date: {receipt_date}")
                    except ValueError as e:
                        print(f"Error parsing receipt_date: {receiptdate}. Error: {e}")
                        raise ValueError("Invalid date format. Expected format: DD/MM/YYYY")

                elif isinstance(receiptdate, datetime):
                    receipt_date = receiptdate
                    print(f"receipt_date is already a datetime object: {receipt_date}")

                # ✅ Use the parsed datetime object
                epoch_timestamp = int(receipt_date.timestamp() * 1000)
                print(f"Converted epoch timestamp: {epoch_timestamp}")

            else:
                print("No receipt_date found in the database or receipt_date is NULL.")
                epoch_timestamp = None
            print("Water Application Create Request: " + request_body_string)
            print("Water Application Created Succesfully with Response: " + response_decoded)
            query2="update eg_ws_service set connectioncategory='General', connectiontype='Non Metered', watersource='GROUNDWATER',connectionexecutiondate='"+str({epoch_timestamp})+"' where connection_id='"+api_response_dict['WaterConnection'][0]['id']+"';"
            query="update eg_ws_connection set applicationstatus = 'CONNECTION_ACTIVATED',status ='Active',connectionno = '"+connectionNumebr+"',action ='ACTIVATE_CONNECTION',"  "oldconnectionno = '" + oldconnectionno + "' " "where applicationno='" +application_no+"' ;"
            query3="update eg_wf_processinstance_v2 set action='ACTIVATE_CONNECTION', status='d81daa52-a4f8-498e-b270-40cc9750818b' where businessid='" +application_no+"' ;"
            query=query+query2+query3
            update_query = f"""UPDATE nangal_migrate_records_uat SET water_query_activate = %s, water_upload_status='COMPLETED',water_upload_req = %s,water_upload_res = %s,new_water_application_number=%s,iswatermigrated='True' WHERE pkwsid = %s"""
            cursor_ws.execute(update_query, (query,request_body_string,response_decoded,application_no,str(pkwsid)))
            connection_db_ws.commit()
            # update_demand_query = "UPDATE nangal_migrate_records_uat SET isconnectionmigrated='True' WHERE id_no ='"+connectionNumebr+"'"
            # cursor_ws.execute(update_demand_query)
            print("Water Application Created Succesfully with Application Number: " + application_no)
        else:
            pkwsid = waterData.water_data['pkwsid']
            update_query = f"""UPDATE nangal_migrate_records_uat SET water_upload_req = %s , water_upload_res = %s, water_upload_status ='ERROR' WHERE pkwsid = %s"""
            cursor_ws.execute(update_query, (request_body_string, response_decoded, str(pkwsid)))
        connection_db_ws.commit()



class conn_holder_details:
    def ownerdetails(self, ownername,ownermobileno):
        # Split the string at commas
        split_result = [element.strip() for element in ownername.split(',')]
        surname = set(["sh", "smt", "mr", "mrs"])

        # Remove specified elements from the split array
        cleaned_elements = [item for item in split_result if item.lower() not in surname]

        split_list_of_dicts = [{'sameAsPropertyAddress': False,
                                    'name': cleaned_element,
                                    'mobileNumber':ownermobileno,
                                    'gender': 'MALE',
                                    'relationship': 'Father',
                                    'fatherOrHusbandName': None,
                                    'correspondenceAddress': None,
                                    'ownerType': 'NONE'
                                    } for cleaned_element in cleaned_elements]


        return json.dumps(split_list_of_dicts)
class conn_holder_details:
    def ownerdetails(self, ownername,ownermobileno):
        # Split the string at commas
        split_result = [element.strip() for element in ownername.split(',')]
        surname = set(["sh", "smt", "mr", "mrs"])
        if ownermobileno==None or ownermobileno=='null' or ownermobileno=='':
            ownermobileno="7777777777"
        else:
            ownermobileno=ownermobileno
        # Remove specified elements from the split array
        cleaned_elements = [item for item in split_result if item.lower() not in surname]
        cleaned_elements = [self.remove_symbols(element) for element in cleaned_elements]

        split_list_of_dicts = [{'sameAsPropertyAddress': False,
                                    'name': cleaned_element,
                                    'mobileNumber':ownermobileno,
                                    'gender': 'MALE',
                                    'relationship': 'Father',
                                    'fatherOrHusbandName': None,
                                    'correspondenceAddress': None,
                                    'ownerType': 'NONE'
                                    } for cleaned_element in cleaned_elements]

        return json.dumps(split_list_of_dicts, ensure_ascii=False).encode('utf8').decode('unicode_escape')

    def remove_symbols(self, ownername):
        pattern = re.compile('[^A-Za-z0-9\s]+')

        # Use the pattern to replace symbols with an empty string
        cleaned_string = re.sub(pattern, '', ownername)
        return cleaned_string

