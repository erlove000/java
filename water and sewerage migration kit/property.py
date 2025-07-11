import json
import psycopg2
import requests

# Database connection (PostgreSQL)
db = psycopg2.connect(
    host="localhost",
    user="postgres",  # Update this if needed
    password="postgres",  # Update this if needed
    dbname="Khanna"  # Update this if needed
)

cursor = db.cursor()

# Step 1: Retrieve property IDs where landarea or usagecategory is NULL
cursor.execute("SELECT propertyid FROM propertydata WHERE landarea IS NULL OR usagecategory IS NULL")
property_ids = cursor.fetchall()

# Define the base API URL
base_api_url = "https://mseva.lgpunjab.gov.in/property-services/property/_search?tenantId=pb.khanna&propertyIds="

# Define the JSON body template (this remains the same as before)
json_body = {
    "RequestInfo": {
        "apiId": "Mihy",
        "ver": ".01",
        "action": "",
        "did": "1",
        "key": "",
        "msgId": "20170310130900|en_IN",
        "requesterId": "",
        "authToken": "6d9f23a1-b49b-41b1-a50c-a12496bd5d80",
        "userInfo": {
            "id": 1296680,
            "userName": "EMP-KHANNA-MIGRATOR",
            "salutation": None,
            "name": "WNS Migrator",
            "gender": "MALE",
            "mobileNumber": "9012345699",
            "emailId": None,
            "altContactNumber": None,
            "pan": None,
            "aadhaarNumber": None,
            "permanentAddress": "Test address",
            "permanentCity": None,
            "permanentPinCode": None,
            "correspondenceAddress": "Test address",
            "correspondenceCity": None,
            "correspondencePinCode": None,
            "alternatemobilenumber": None,
            "active": True,
            "locale": None,
            "type": "EMPLOYEE",
            "accountLocked": False,
            "accountLockedDate": 0,
            "fatherOrHusbandName": "WnS",
            "relationship": None,
            "signature": None,
            "bloodGroup": None,
            "photo": None,
            "identificationMark": None,
            "createdBy": 10981,
            "lastModifiedBy": 10981,
            "tenantId": "pb.nangal",
            "roles": [
                {"code": "SW_APPROVER", "name": "SW Approver", "tenantId": "pb.khanna"},
                {"code": "PT_REC_CANCEL", "name": "PT Receipt Cancellation", "tenantId": "pb.khanna"},
                # Add other roles as required...
            ],
            "uuid": "aa7ccddd-40e5-452c-b9d1-f28750c10d7b",
            "createdDate": "09-05-2021 00:49:06",
            "lastModifiedDate": "09-05-2021 00:49:06",
            "dob": "7/10/1994",
            "pwdExpiryDate": "07-08-2021 00:49:06"
        }
    }
}

for (propertyid,) in property_ids:
    # Step 2: Construct the URL by appending the current property ID to the base URL
    api_url = f"{base_api_url}{propertyid}"

    # Step 3: Make an API call with the constructed URL
    response = requests.post(api_url, json=json_body)

    if response.status_code == 200:
        data = response.json()

        # Step 4: Extract landarea and usagecategory from the 'Properties' list
        properties = data.get("Properties", [])
        if properties:
            property_data = properties[0]  # Assuming we are dealing with the first property in the list
            landarea = property_data.get("landArea")
            usagecategory = property_data.get("usageCategory")

            if landarea is not None or usagecategory is not None:
                # Step 5: Update the database if values are NULL
                cursor.execute(
                    """
                    UPDATE propertydata 
                    SET landarea = COALESCE(%s, landarea), usagecategory = COALESCE(%s, usagecategory)
                    WHERE propertyid = %s
                    """,
                    (landarea, usagecategory, propertyid)
                )
                db.commit()
        else:
            print(f"No property data found for property ID {propertyid}")
    else:
        print(f"Failed to fetch data for property ID {propertyid}, Status Code: {response.status_code}")

# Close the cursor and database connection
cursor.close()
db.close()
