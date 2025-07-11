
# from common import *
from dbconfig import *
import requests


from dbconfig import batch
from uploader.waterdemand import waterdemand
# from uploader.seweragedemand import  seweragedemand
# postgresql_select_Query = """
# select row_to_json(cd) from nangal_migrate_records_uat as cd where pkwsid='2005M01238' and iswatermigrated='True' """.format(batch)

postgresql_select_Query = """
select row_to_json(cd) from nangal_migrate_records_prod as cd where isconmig='True' and iswatermigrated='True' and iswaterdemanduploaded='False'; """.format(batch)



def main():
    continue_processing = True
    dry_run=True # only one record to migrate
    access_token = superuser_login()["access_token"]
    import time
    # continue_processing = False
    while continue_processing:
        print(postgresql_select_Query)
        cursor_ws.execute(postgresql_select_Query)
        data = cursor_ws.fetchmany(int(batch_size))

        # continue_processing = False
        if not data:
            print("No more data to process. Script exiting")
            continue_processing = False
            cursor_ws.close()
            connection_db_ws.close()

        for row in data:
            json_data = row[0]
            uuid = json_data["uuid"]
            type = json_data["conn_type"]
            iswatermigrated=json_data["iswaterdemanduploaded"]
            isseweragemigrated = json_data["isseweragedemanduploaded"]
            connnumber = json_data["pkwsid"]
            print('Processing {}'.format(uuid))
            print("Searching Connection  In Digit : "+ connnumber)
            headers = {
                "Content-Type": "application/json",
            }
            url = ('https://mseva.lgpunjab.gov.in/ws-services/wc/_search?tenantId=' +Employee_tenatid+ '&isConnectionSearch=true&connectionNumber='+ connnumber+'')
            request_body_search = {
                'RequestInfo': {
                    'apiId': 'Rainmaker',
                    'ver': '.01',
                    'action': '_create',
                    'did': '1',
                    'key': '',
                    'msgId': '20170310130900|en_IN',
                    'requesterId': '',
                    'authToken': access_token
                }
            }
            water_search_response = requests.post(url, headers=headers, data=json.dumps(request_body_search))
            water_response = water_search_response.json()

            headers = {
                "Content-Type": "application/json",
            }
            url = (
                        'https://mseva.lgpunjab.gov.in/sw-services/swc/_search?tenantId=' + Employee_tenatid + '&isConnectionSearch=true&connectionNumber='+connnumber)
            # + connnumber)
            request_body_search = {
                'RequestInfo': {
                    'apiId': 'Rainmaker',
                    'ver': '.01',
                    'action': '_create',
                    'did': '1',
                    'key': '',
                    'msgId': '20170310130900|en_IN',
                    'requesterId': '',
                    'authToken': access_token
                }
            }
            sewerage_search_response = requests.post(url, headers=headers, data=json.dumps(request_body_search))
            sewerage_response = sewerage_search_response.json()

            if sewerage_search_response.status_code==200 or water_search_response.status_code==200:
                if type=="Water & Sewer":
                    print("Connection Is of Both Type that is : "+ type+ " Conn Number : "+connnumber)
                    if (iswatermigrated=="False" and water_response['WaterConnection'][0]):
                        waterdemand(water_response, json_data)
                    else:
                        print("SEARCH WATER CONNECTION IS GIVING NULL"+str(water_response)+" OR WATER CONNECTION IS ALREADY MIGRATED "+iswatermigrated+"")
                    if(isseweragemigrated=="False" and  sewerage_response['SewerageConnections'][0]):
                        seweragedemand(sewerage_response, json_data)
                    else:
                        print("SEARCH SEWERAGE CONNECTION IS GIVING NULL" + str(sewerage_response) + " OR WATER CONNECTION IS ALREADY MIGRATED " + isseweragemigrated + "")

                elif type=="WATER" and iswatermigrated=="False" and water_response['WaterConnection'][0]:
                    print("Connection Is of Water Type that is : "+ type+ " Conn Number : "+connnumber)
                    waterdemand(water_response, json_data)

                elif type == "Sewer" and isseweragemigrated=="True" and  sewerage_response['SewerageConnections'][0]:
                    print("Connection Is of Sewerage Type that is : "+ type+ " Conn Number : "+connnumber)
                    seweragedemand(sewerage_response, json_data)
                # else:
                    # if type=="WATER":
                        # print("SEARCH WATER CONNECTION IS GIVING NULL" + water_response + " OR WATER CONNECTION IS ALREADY MIGRATED " + iswmigrated + "")
                    # if type=="Sewer":
                    #     print(
                            # "SEARCH SEWERAGE CONNECTION IS GIVING NULL" + sewerage_response + " OR WATER CONNECTION IS ALREADY MIGRATED " + iswmigrated + "")

            receipt_query = "Select * from nangal_migrate_records_prod where pkwsid='" + connnumber + "'"
            cursor_ws.execute(receipt_query)
            data = cursor_ws.fetchmany()
            for row in data:
                iswmigrated = row[35]
                issmigrated = row[27]
                print ("Is Water Demand , Receipt migrated "+iswmigrated)
                print ("Is Sewerage Demand , Receipt migrated "+issmigrated)
            if ( type =="Water & Sewer" and  issmigrated == "True" and  iswmigrated == "True"):
                update_query_seweragetable = "UPDATE nangal_migrate_records_prod SET iswholedemandmigrated='True' WHERE pkwsid='" +connnumber+ "';"
                print("Migrated Successfully of Connection Type " + type + " Having  Consumer Code " + connnumber)
                cursor_ws.execute(update_query_seweragetable)
                connection_db_ws.commit()
            elif (type == "WATER" and iswmigrated == "True"):
                update_query_seweragetable = "UPDATE nangal_migrate_records_prod SET iswholedemandmigrated='True' WHERE pkwsid='" + connnumber + "';"
                cursor_ws.execute(update_query_seweragetable)
                connection_db_ws.commit()
                print ("Migrated Successfully of Connection Type "+type+ " Having  Consumer Code "+ connnumber)
            elif (type == "Sewer" and issmigrated == "True"):
                update_query_seweragetable = "UPDATE nangal_migrate_records_prod SET iswholedemandmigrated='True' WHERE pkwsid='" + connnumber + "';"
                cursor_ws.execute(update_query_seweragetable)
                connection_db_ws.commit()
                print("Migrated Successfully of Connection Type " + type + " Having  Consumer Code " + connnumber)

            if dry_run:
                print("dry run allowed single record processing")
                continue_processing=True
                break


if __name__ == "__main__":
    main()

