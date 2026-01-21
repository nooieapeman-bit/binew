from config import create_ssh_tunnel, get_db_connection, PRODUCT_CODE_MAPPING
from datetime import datetime

def print_remaining_missing_products():
    end_of_2024 = 1735689600
    # Now the keys are the IDs
    codes = list(PRODUCT_CODE_MAPPING.keys())
    
    print(f"Fetching details for the ~26 remaining records not in mapping...")
    
    with create_ssh_tunnel() as server:
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                # Get the top 9 that were already summarized to exclude them
                top_pids = [
                    '18e75ca570f308d530eb2060928af51e',
                    'cd93db1fe522bfd23b24c1b53186ba66',
                    'd87851136de8c85bfb2dd63c58089165',
                    '8401c476dae011eaa280067a3e6cf430',
                    '3992f872dae011eaa280067a3e6cf430',
                    '83a762155aae410159f5b95d4b36e93b',
                    '2cb7dc9ddadf11ea988612a3c079152e',
                    '2182e946dae011eaa280067a3e6cf430',
                    '77a6f17edae111eaa280067a3e6cf430'
                ]
                
                exclude_list = codes + top_pids
                
                sql = """
                    SELECT o.product_id, o.product_name, COUNT(DISTINCT ci.id) as count
                    FROM cloud_info ci
                    JOIN `order` o ON ci.order_id = o.order_id
                    WHERE ci.start_time < %s 
                      AND ci.end_time >= %s 
                      AND o.product_id NOT IN %s
                    GROUP BY o.product_id, o.product_name
                    ORDER BY count DESC
                """
                cur.execute(sql, (end_of_2024, end_of_2024, exclude_list))
                rows = cur.fetchall()
                
                print("\nRemaining Records (Exclude top 9 and mapping):")
                print(f"{'Product ID':<35} | {'Product Name':<40} | Count")
                print("-" * 85)
                for pid, pname, count in rows:
                    print(f"{str(pid):<35} | {str(pname):<40} | {count}")
                    
        finally:
            conn.close()

if __name__ == "__main__":
    print_remaining_missing_products()
