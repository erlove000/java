from datetime import datetime

from dbconfig import *


class SewerageConnection:
    def __init__(self, property_reponse, sewerage_data):
        self.property_reponse = property_reponse
        self.sewerage_data = sewerage_data
        self.createsewerage(self)

    def createsewerage(self, sewerageData):
        print("Creating Sewerage Application")
        connType = sewerageData.sewerage_data['conn_type']

        # Simulating data retrieved from the database
        owner_name = sewerageData.sewerage_data['ownername']

        # Remove "/" and everything after "("
        owner_name = re.sub(r'/| \(.*', '', owner_name)

        if (sewerageData.sewerage_data['mobile'] == None or sewerageData.sewerage_data['mobile'] == ''):
            sewerageData.sewerage_data['mobile'] = '9999999999'
        else:
            sewerageData.sewerage_data['mobile'] = sewerageData.sewerage_data['mobile']


        if (connType == "B"):
            iswaterconn = True
            issewerageconn = True
            isname = "Water And Sewerage"
        elif connType in ("S", "SEWRAGE"):
            iswaterconn = False
            issewerageconn = True
            isname = "Sewerage"
        conn_holder_detail = conn_holder_details()
        conn_holder = conn_holder_detail.ownerdetails(sewerageData.sewerage_data['ownername'], sewerageData.sewerage_data['mobile'])
        ConnHolderDetail = json.loads(conn_holder)
        url = "https://mseva.lgpunjab.gov.in/sw-services/swc/_create"
        headers = {
            "Content-Type": "application/json",
        }
        request_body = \
            {
                'RequestInfo': {
                    'apiId': 'Mihy',
                    'ver': '.01',
                    'action': '',
                    'did': '1',
                    'key': '',
                    'msgId': '20170310130900|en_IN',
                    'requesterId': '',
                    "authToken": superuser_login()["access_token"]
                },
                'sewerageConnection':
                    {
                        'water': iswaterconn,
                        'sewerage': issewerageconn,
                        'connectionHolders': ConnHolderDetail,
                        'property': self.property_reponse[0],
                        "service": isname,
                        "roadCuttingArea": None,
                        "proposedWaterClosets": None,
                        "proposedToilets": None,
                        "noOfTaps": None,
                        "noOfWaterClosets": None,
                        "noOfToilets": None,
                        "proposedTaps": None,
                        "propertyId": self.property_reponse[0]['propertyId'],
                        "additionalDetails": {
                            "initialMeterReading": None,
                            "detailsProvidedBy": "",
                            "isexempted": False,
                            "billingType": None,
                            "billingAmount": None,
                            "connectionCategory": None,
                            "ledgerId": None,
                            "avarageMeterReading": None,
                            "meterMake": None,
                            "compositionFee": None,
                            "userCharges": None,
                            "othersFee": None,
                            "unitUsageType": None,
                            "adhocPenalty": None,
                            "adhocPenaltyComment": None,
                            "adhocPenaltyReason": None,
                            "adhocRebate": None,
                            "adhocRebateComment": None,
                            "adhocRebateReason": None,
                            "estimationFileStoreId": None,
                            "sanctionFileStoreId": None,
                            "estimationLetterDate": None,
                            "locality": self.property_reponse[0]['address']['locality']['code'],
                            'pkwsid':sewerageData.sewerage_data['pkwsid'],
                            'block':sewerageData.sewerage_data['block'],
                            'propertyno':sewerageData.sewerage_data['propertyno'],
                            'uidno':sewerageData.sewerage_data['uidno'],
                            'email':sewerageData.sewerage_data['email'],
                            'remarks':sewerageData.sewerage_data['remarks'],
                            'tariff_type':sewerageData.sewerage_data['tariff_type'],
                            'conn_type':sewerageData.sewerage_data['conn_type'],
                            'disconn_status':sewerageData.sewerage_data['disconn_status'],
                            'exempted':sewerageData.sewerage_data['exempted'],
                            'area_in_sqy':sewerageData.sewerage_data['area_in_sqy'],
                            # 'amount' : sewerageData.sewerage_data['amount']
                            "receiptnumber": sewerageData.sewerage_data['receiptnumber'],
                            "receiptdate": sewerageData.sewerage_data['receiptdate'],
                            "securitydeposit": sewerageData.sewerage_data['securitydeposit'],
                            "connectionfee": sewerageData.sewerage_data['connectionfee'],
                            "others": sewerageData.sewerage_data['others'],
                            "roadcuttingcharges": sewerageData.sewerage_data['roadcuttingcharges'],
                            "arealocalitysector": sewerageData.sewerage_data['arealocalitysector'],
                            "erp_locality": sewerageData.sewerage_data['erp_locality'],
                            "pt_loc_code": sewerageData.sewerage_data['pt_loc_code'],
                            "applicationnumber": sewerageData.sewerage_data['applicationnumber'],
                            "applicationdate": sewerageData.sewerage_data['applicationdate'],
                            # "waterSubUsageType": sewerageData.sewerage_data['waterSubUsageType'],
                            # "dischargeConnection": sewerageData.sewerage_data['dischargeConnection']
                            "waterSubUsageType": sewerageData.sewerage_data.get('watersubusagetype', ''),
                            "dischargeConnection": sewerageData.sewerage_data.get('dischargeconnection', '')
                        },
                        "tenantId": self.property_reponse[0]['tenantId'],
                        "processInstance": {
                            "action": "INITIATE"
                        }

                    }
            }
        response = requests.post(url, headers=headers, data=json.dumps(request_body))

        response_decoded = response.content.decode('utf-8')
        response_decoded = json.loads(response_decoded)
        response_decoded = json.dumps(response_decoded, indent=2)
        request_body_string = json.dumps(request_body)
        uuid = sewerageData.sewerage_data['uuid']

        if (response.status_code == 200):
            api_response_dict = response.json()
            application_no = api_response_dict['SewerageConnections'][0]['applicationNo']
            url = 'https://mseva.lgpunjab.gov.in/sw-services/swc/_search?tenantId=' + self.property_reponse[0][
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
            connectionNumebr = sewerageData.sewerage_data['pkwsid']

            print("Sewerage Application Create Request: " + request_body_string)
            print("Sewerage Application Created Succesfully with Response: " + response_decoded)

            # Fetch receipt_date from the database based on pkwsid
            pkwsid = sewerageData.sewerage_data['pkwsid']
            #cursor_ws.execute("SELECT receipt_date FROM nangal_migrate_records_uat WHERE pkwsid = %s", (pkwsid,))
            cursor_ws.execute(
                "SELECT receiptdate FROM nangal_migrate_records_prod WHERE pkwsid = %s AND isconmig IS NULL",
                (pkwsid,))
            result = cursor_ws.fetchone()

            if result and result[0]:
                receiptdate = result[0]
                print(f"Fetched receipt_date from DB: {receiptdate}")

                if isinstance(receiptdate, str):
                    try:
                        receiptdate = receiptdate.strip()
                        receiptdate = receiptdate.replace("//", "/")

                        # Handle different date formats
                        if "-" in receiptdate and ":" in receiptdate:
                            receiptdate = datetime.strptime(receiptdate, "%d-%m-%Y %H:%M")  # Format: DD-MM-YYYY HH:MM
                        elif "-" in receiptdate:
                            receiptdate = datetime.strptime(receiptdate, "%d-%m-%Y")  # Format: DD-MM-YYYY
                        elif "/" in receiptdate:
                            receiptdate = datetime.strptime(receiptdate, "%d/%m/%Y")  # Format: DD/MM/YYYY

                        print(f"Parsed receipt_date: {receiptdate}")
                    except ValueError as e:
                        print(f"Error parsing receipt_date: {receiptdate}. Error: {e}")
                        raise ValueError("Invalid date format. Expected format: DD/MM/YYYY or DD-MM-YYYY HH:MM")

                elif isinstance(receiptdate, datetime):
                    print(f"receipt_date is already a datetime object: {receiptdate}")


                epoch_timestamp = int(receiptdate.timestamp() * 1000)
                print(f"Converted epoch timestamp: {epoch_timestamp}")

            else:
                print("No receipt_date found in the database or receipt_date is NULL.")
                epoch_timestamp = None  # Handle the absence of a receipt_date gracefully
            query = "update eg_sw_connection set applicationstatus = 'CONNECTION_ACTIVATED',status ='Active',connectionno = '" + connectionNumebr + "',action ='ACTIVATE_CONNECTION' where applicationno='" + application_no + "' ;"
            query2 = "update eg_sw_service set  connectiontype='Non Metered', connectionexecutiondate='"+str({epoch_timestamp})+"'  where connection_id='" +search_response['SewerageConnections'][0]['id'] + "';"
            query3="update eg_wf_processinstance_v2 set action='ACTIVATE_CONNECTION', status='a3cc91cc-8cda-4334-8afc-e403e304d741' where businessid='" +application_no+"' ;"
            query=query+query2+query3
            update_query = f"""UPDATE nangal_migrate_records_prod SET sewerage_query_activate = %s, sewerage_upload_status='COMPLETED',sewerage_upload_req = %s,sewerage_upload_res = %s,new_sewerage_application_number=%s WHERE pkwsid = %s"""
            cursor_ws.execute(update_query, (query, request_body_string, response_decoded, application_no, str(pkwsid)))
            connection_db_ws.commit()
            # update_demand_query = "UPDATE nangal_migrate_records_prod SET isconnectionmigrated='True' WHERE id_no ='" + connectionNumebr + "'"
            # cursor_ws.execute(update_demand_query)
            print("Sewerage Application Created Succesfully with Application Number: " + application_no)
        else:
            update_query = f"""UPDATE nangal_migrate_records_prod SET sewerage_upload_req = %s , sewerage_upload_res = %s,sewerage_upload_status ='ERROR' WHERE uuid = %s"""
            cursor_ws.execute(update_query, (request_body_string,response_decoded, uuid))
        connection_db_ws.commit()


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
