import requests
import psycopg2  # PostgreSQL connector


def get_connection_nos():
    try:
        conn = psycopg2.connect(
            host="localhost",
            user="postgres",
            password="postgres",
            database="MeterQ3Data"
        )
        cursor = conn.cursor()
        #cursor.execute("SELECT id_no FROM meter_billing WHERE isdead= 'TRUE' and islocked='FALSE' and prev_rdg is null")
        cursor.execute("SELECT id_no FROM meter_billing_missing_last WHERE prev_rdg is null")
        connection_nos = [row[0] for row in cursor.fetchall()]
        cursor.close()
        conn.close()
        print(f"Fetched connection numbers: {connection_nos}")  # Debug log
        return connection_nos
    except psycopg2.Error as err:
        print(f"Database error: {err}")
        return []


def fetch_current_reading(id_no):
    """Call the API and get the current reading for a given connection number."""
    url = f"https://mseva.lgpunjab.gov.in/ws-calculator/meterConnection/_search?tenantId=pb.amritsar&connectionNos={id_no}&offset=0"
    headers = {
        "accept": "application/json, text/plain, */*",
        "content-type": "application/json;charset=UTF-8",
    }
    payload = {
        "RequestInfo": {
            "apiId": "Rainmaker",
            "ver": ".01",
            "action": "_search",
            "did": "1",
            "msgId": "20170310130900|en_IN",
            "authToken": "a97f12ae-5871-4e58-96d4-a38fd0e21d62"
        }
    }
    try:
        response = requests.post(url, json=payload, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()
        print(f"API response for id_no {id_no}: {data}")  # Debug log
        if "meterReadings" in data and data["meterReadings"]:
            latest_reading = max(data["meterReadings"], key=lambda x: x["currentReadingDate"])
            return latest_reading["currentReading"]
        else:
            print(f"No meter readings found for id_no {id_no}")  # Debug log
    except requests.exceptions.RequestException as err:
        print(f"API request error for id_no {id_no}: {err}")
    except ValueError:
        print(f"Invalid JSON response for id_no {id_no}")
    return None


def update_current_reading(id_no, current_reading):
    try:
        conn = psycopg2.connect(
            host="localhost",
            user="postgres",
            password="postgres",
            database="MeterQ3Data"
        )
        cursor = conn.cursor()
        print(f"Updating id_no {id_no} with reading {current_reading}")  # Debug log
        cursor.execute("UPDATE meter_billing_missing_last SET prev_rdg = %s WHERE id_no = %s", (current_reading, id_no))
        conn.commit()

        if cursor.rowcount > 0:
            print(f"Successfully updated id_no {id_no} with reading {current_reading}")  # Debug log
        else:
            print(f"No rows updated for id_no {id_no}. Check if the id_no exists in the table.")  # Debug log

        cursor.close()
        conn.close()
    except psycopg2.Error as err:
        print(f"Database update error for id_no {id_no}: {err}")


def main():
    connection_nos = get_connection_nos()
    if not connection_nos:
        print("No connection numbers found. Exiting...")
        return

    for id_no in connection_nos:
        current_reading = fetch_current_reading(id_no)
        if current_reading is not None:
            update_current_reading(id_no, current_reading)
        else:
            print(f"No reading found for id_no {id_no}")


if __name__ == "__main__":
    main()
