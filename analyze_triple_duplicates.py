from config import create_ssh_tunnel, get_db_connection

def analyze_duplicate_orders():
    print("Analyzing order_id duplicates with count > 2...")
    with create_ssh_tunnel() as server:
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                # 1. Count groups
                sql_count = """
                    SELECT COUNT(*) FROM (
                        SELECT order_id FROM `order` 
                        WHERE order_id IS NOT NULL AND order_id != ''
                        GROUP BY order_id 
                        HAVING COUNT(*) > 2
                    ) as t
                """
                cur.execute(sql_count)
                group_count = cur.fetchone()[0]
                print(f"Number of groups where order_id count > 2: {group_count}")

                if group_count > 0:
                    # 2. Fetch some samples with full details
                    print("\nFetching sample details for these groups...")
                    sql_samples = """
                        SELECT order_id, COUNT(*) as cnt 
                        FROM `order` 
                        WHERE order_id IS NOT NULL AND order_id != ''
                        GROUP BY order_id 
                        HAVING cnt > 2 
                        LIMIT 5
                    """
                    cur.execute(sql_samples)
                    samples = cur.fetchall()
                    
                    for row in samples:
                        oid = row[0]
                        count = row[1]
                        print(f"\nOrder ID: {oid} (Count: {count})")
                        
                        # Fetch full history for this order_id
                        sql_details = """
                            SELECT id, uid, product_name, status, amount, 
                                   FROM_UNIXTIME(submit_time) as sub_time, 
                                   FROM_UNIXTIME(pay_time) as p_time,
                                   subscribe_id
                            FROM `order`
                            WHERE order_id = %s
                            ORDER BY submit_time ASC
                        """
                        cur.execute(sql_details, (oid,))
                        details = cur.fetchall()
                        print(f"{'Int ID':<10} | {'UID':<15} | {'Status':<6} | {'Amount':<8} | {'Submit':<20} | {'PayTime':<20}")
                        print("-" * 90)
                        for d in details:
                            print(f"{str(d[0]):<10} | {str(d[1]):<15} | {str(d[3]):<6} | {str(d[4]):<8} | {str(d[5]):<20} | {str(d[6]):<20}")

        finally:
            conn.close()

if __name__ == "__main__":
    analyze_duplicate_orders()
