import json
import psycopg2
import requests

# Database connection (PostgreSQL)
db = psycopg2.connect(
    host="localhost",
    user="postgres",
    password="postgres",
    dbname="Fazilka_Migration"
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
base_api_url = "https://mseva.lgpunjab.gov.in/property-services/property/_search?tenantId=pb.fazilka&propertyIds="

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
        "authToken": "1f4a053b-d69f-4480-a0f8-697d21d052b1",
        "userInfo": {
            "id": 1296680,
            "userName": "EMP-FAZILKA-MIGRATOR",
            "name": "WNS Migrator",
            "gender": "MALE",
            "mobileNumber": "9012345699",
            "type": "EMPLOYEE",
            "active": True,
            "tenantId": "pb.fazilka",
            "roles": [
                {"code": "SW_APPROVER", "name": "SW Approver", "tenantId": "pb.fazilka"},
                {"code": "PT_REC_CANCEL", "name": "PT Receipt Cancellation", "tenantId": "pb.fazilka"}
            ],
            "uuid": "bb1ec6d4-9164-4247-baef-6246682511fc"
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
