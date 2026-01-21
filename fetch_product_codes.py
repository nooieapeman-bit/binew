from config import create_ssh_tunnel, get_db_connection, VALID_PRODUCTS

def get_product_codes():
    print("Fetching product codes from set_meal...")
    with create_ssh_tunnel() as server:
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                # Need to be careful with spaces in VALID_PRODUCTS
                cleaned_names = [name.strip() for name in VALID_PRODUCTS]
                sql = "SELECT name, code FROM `set_meal` WHERE TRIM(name) IN %s"
                cur.execute(sql, (cleaned_names,))
                results = cur.fetchall()
                
                mapping = {}
                for name, code in results:
                    mapping[name.strip()] = code.strip()
                
                print("\nFound Mappings:")
                for name in VALID_PRODUCTS:
                    s_name = name.strip()
                    code = mapping.get(s_name, "NOT FOUND")
                    print(f"'{name}' -> {code}")
                    
                # Print as a python dictionary format for easy copy-paste just in case
                print("\nDictionary Format:")
                import json
                print(json.dumps(mapping, indent=4))
        finally:
            conn.close()

if __name__ == "__main__":
    get_product_codes()
