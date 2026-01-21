from config import create_ssh_tunnel, get_db_connection, PRODUCT_CODE_MAPPING
from datetime import datetime

def check_missing_product_ids():
    end_of_2024 = 1735689600
    codes = list(PRODUCT_CODE_MAPPING.values())
    
    print(f"Checking for product_ids NOT in the mapping for 2024-end active subscriptions...")
    
    with create_ssh_tunnel() as server:
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                # Count missing records
                sql = """
                    SELECT COUNT(DISTINCT ci.id)
                    FROM cloud_info ci
                    JOIN `order` o ON ci.order_id = o.order_id
                    WHERE ci.start_time < %s 
                      AND ci.end_time >= %s 
                      AND o.product_id NOT IN %s
                """
                cur.execute(sql, (end_of_2024, end_of_2024, codes))
                missing_count = cur.fetchone()[0]
                print(f"Total matching cloud_info records with product_id NOT in mapping: {missing_count}")
                
                # Breakdown of missing ones
                sql_breakdown = """
                    SELECT o.product_id, o.product_name, COUNT(DISTINCT ci.id) as count
                    FROM cloud_info ci
                    JOIN `order` o ON ci.order_id = o.order_id
                    WHERE ci.start_time < %s 
                      AND ci.end_time >= %s 
                      AND o.product_id NOT IN %s
                    GROUP BY o.product_id, o.product_name
                    ORDER BY count DESC
                """
                cur.execute(sql_breakdown, (end_of_2024, end_of_2024, codes))
                rows = cur.fetchall()
                
                print("\nBreakdown (Product ID | Product Name | Count):")
                print("-" * 80)
                for pid, pname, count in rows:
                    print(f"{str(pid):<35} | {str(pname):<30} | {count}")
                    
        finally:
            conn.close()

if __name__ == "__main__":
    check_missing_product_ids()
