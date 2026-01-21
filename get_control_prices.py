
import pymysql
from config import create_ssh_tunnel, get_db_connection

pkg_names = [
    '14-day video history event recording annually',
    '14-day history event recording monthly',
    '30-day video history event recording annually',
    '30-day video history event recording monthly',
    '7-day video history CVR recording monthly',
    '30-day video history event recording AI monthly',
    '30-day video history event recording pro monthly',
    '7-day video history CVR recording annually',
    '14-day video history CVR recording monthly',
    '30-day video history event recording AI annually',
    '30-day video history event recording pro annually',
    '14-day video history CVR recording annually',
    '14-day video history CVR recording AI monthly',
    '14-day video history CVR recording pro monthly',
    '14-day video history CVR recording AI annually'
]

# Note: Some package names might have trailing spaces or slight variations in DB.
# We will use LIKE or trim in SQL to match.

with create_ssh_tunnel() as server:
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            print(f"Fetching EUR prices for {len(pkg_names)} packages...")
            
            # 1. Get Code for Names
            # We use a loop or dynamic SQL. 
            # To be safe against variations, let's fetch all set_meals and match in python
            cur.execute("SELECT name, code FROM set_meal")
            rows = cur.fetchall()
            
            name_code_map = {}
            # Normalize DB names (strip)
            db_map = {r[0].strip(): r[1] for r in rows}
            
            found_codes = []
            print("\nMatching Codes:")
            for p in pkg_names:
                clean_p = p.strip()
                if clean_p in db_map:
                    code = db_map[clean_p]
                    found_codes.append(code)
                    # print(f"Found: '{clean_p}' -> {code}")
                else:
                    # Try partial match or manual fix if needed?
                    # The previous output came from DB so they should exist.
                    # Maybe issue with 'recording annually  ' (2 spaces)
                    # Let's try matching with existing keys
                    match = None
                    for db_name in db_map.keys():
                        if db_name == clean_p:
                            match = db_map[db_name]
                            break
                    if match:
                        found_codes.append(match)
                    else:
                        print(f"WARNING: Could not find code for '{p}'")

            if not found_codes:
                print("No codes found.")
                exit()
            
            # 2. Get Price for these Codes
            placeholders = ', '.join(['%s'] * len(found_codes))
            sql_price = f"""
                SELECT sm.name, smp.price
                FROM set_meal_price smp
                JOIN set_meal sm ON smp.set_meal_code = sm.code
                WHERE smp.set_meal_code IN ({placeholders})
                  AND smp.currency = 'EUR'
            """
            cur.execute(sql_price, found_codes)
            
            results = cur.fetchall()
            price_map = {r[0].strip(): r[1] for r in results}
            
            print("\nPackage Prices (EUR):")
            print(f"{'Package Name':<50} | {'Price (EUR)':<10}")
            print("-" * 65)
            
            for p in pkg_names:
                cln = p.strip()
                price = price_map.get(cln, 'N/A')
                print(f"{cln:<50} | {price:<10}")

    finally:
        conn.close()
