import json
import psycopg2
import requests

# Database connection (PostgreSQL)
db = psycopg2.connect(
    host="localhost",
    user="postgres",
    password="postgres",
    dbname="Khanna"
)
cursor = db.cursor()

# Step 1: Get properties where any of the target fields are NULL
cursor.execute("""
    SELECT propertyid 
    FROM propertydata 
    WHERE landarea IS NULL 
       OR usagecategory IS NULL 
       OR mobile IS NULL 
       OR "mobileNumber" IS NULL
""")
property_ids = cursor.fetchall()

# Define the API base URL
base_api_url = "https://mseva.lgpunjab.gov.in/property-services/property/_search?tenantId=pb.khanna&propertyIds="

# RequestInfo template
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
            "name": "WNS Migrator",
            "gender": "MALE",
            "mobileNumber": "9012345699",
            "type": "EMPLOYEE",
            "active": True,
            "tenantId": "pb.nangal",
            "roles": [
                {"code": "SW_APPROVER", "name": "SW Approver", "tenantId": "pb.khanna"},
                {"code": "PT_REC_CANCEL", "name": "PT Receipt Cancellation", "tenantId": "pb.khanna"}
            ],
            "uuid": "aa7ccddd-40e5-452c-b9d1-f28750c10d7b"
        }
    }
}

# Step 2: Loop through each property ID
for (propertyid,) in property_ids:
    api_url = f"{base_api_url}{propertyid}"
    response = requests.post(api_url, json=json_body)

    if response.status_code == 200:
        data = response.json()
        properties = data.get("Properties", [])

        if properties:
            prop = properties[0]
            landarea = prop.get("landArea")
            usagecategory = prop.get("usageCategory")

            # Extract owner mobileNumber
            owner_mobile = None
            owners = prop.get("owners", [])
            if owners:
                owner_mobile = owners[0].get("mobileNumber")

            # Extract legacy mobile from additionalDetails
            legacy_mobile = None
            legacy_info = prop.get("additionalDetails", {}).get("legacyInfo", {})
            legacy_mobile = legacy_info.get("mobile")

            # Step 3: Update record in database
            if any([landarea, usagecategory, legacy_mobile, owner_mobile]):
                cursor.execute(
                    """
                    UPDATE propertydata 
                    SET 
                        landarea = COALESCE(%s, landarea),
                        usagecategory = COALESCE(%s, usagecategory),
                        mobile = COALESCE(%s, mobile),
                        "mobileNumber" = COALESCE(%s, "mobileNumber")
                    WHERE propertyid = %s
                    """,
                    (landarea, usagecategory, legacy_mobile, owner_mobile, propertyid)
                )
                db.commit()
        else:
            print(f"No property data found for property ID: {propertyid}")
    else:
        print(f"API failed for property ID {propertyid} with status code {response.status_code}")

# Step 4: Close resources
cursor.close()
db.close()
