
import pymysql
from config import create_ssh_tunnel, get_db_connection

PRODUCT_CODE_MAPPING = {
    "04ddc7037a5c1643e4b70b8e7511e446": "14-day video history CVR recording monthly",
    "25ef29e69266a18e4e62a9abfb24906e": "7-day video history CVR recording monthly",
    "2d45e3725a5340568d134b65d0c7caa2": "14-day history event recording monthly",
    "39ead1f70dc714c3b77f59cff2f9b6a3": "14-day video history CVR recording annually",
    "3d0ac7655d04b572480900cc01fb598b": "30-day video history event recording AI annually",
    "675dab39bc46e7c9f19976c6df9cab83": "30-day video history event recording annually",
    "689860a9db2aac4e0375d2edbaf73faf": "14-day video history event recording annually",
    "71d7bf50084b13308df8baa5d0ab67b4": "30-day video history event recording monthly",
    "72ae5ef6671ee995ce33e0f66b46c7f9": "14-day video history CVR recording AI annually",
    "72ea10ef0bae90d823529d12ead51a96": "14-day video history CVR recording pro annually",
    "8590980d14c303bb7ef93bb30432901d": "14-day video history CVR recording AI monthly",
    "a4a1ccf545b9f7e1c9dede93d2aae78f": "30-day video history event recording pro annually",
    "c9f452e0f08e80c1093a82616cc946a4": "30-day video history event recording AI monthly",
    "d90d40f341f05593daf9d4c4702f8bee": "14-day video history CVR recording pro monthly",
    "eb34f5845642ed10ab2d91703baf28e3": "30-day video history event recording pro monthly",
    "ef8d5c18b133ebf0c514f7c9188ca6a3": "7-day video history CVR recording annually"
}

codes = list(PRODUCT_CODE_MAPPING.keys())

with create_ssh_tunnel() as server:
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            placeholders = ', '.join(['%s'] * len(codes))
            sql = f"""
                SELECT set_meal_code, currency, price 
                FROM set_meal_price 
                WHERE set_meal_code IN ({placeholders}) 
                  AND currency IN ('EUR', 'GBP', 'USD')
            """
            cur.execute(sql, codes)
            rows = cur.fetchall()
            
            # price_data[code][currency] = price
            price_data = {code: {'EUR': 'N/A', 'GBP': 'N/A', 'USD': 'N/A'} for code in codes}
            for code, curr, price in rows:
                if code in price_data and curr in price_data[code]:
                    price_data[code][curr] = float(price)
            
            print("| Package Name | EUR | GBP | USD |")
            print("| :--- | :--- | :--- | :--- |")
            for code in codes:
                name = PRODUCT_CODE_MAPPING[code]
                eur = price_data[code]['EUR']
                gbp = price_data[code]['GBP']
                usd = price_data[code]['USD']
                print(f"| {name} | {eur} | {gbp} | {usd} |")
                
    finally:
        conn.close()
