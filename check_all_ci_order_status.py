from config import create_ssh_tunnel, get_db_connection

def check_all_ci_order_status():
    print("Checking status distribution for all orders linked to cloud_info...")
    with create_ssh_tunnel() as server:
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                # Distribution of order statuses
                sql = """
                    SELECT o.status, COUNT(*) 
                    FROM cloud_info ci
                    JOIN `order` o ON ci.order_id = o.order_id
                    GROUP BY o.status
                """
                cur.execute(sql)
                results = cur.fetchall()
                
                print("\nOrder Status Distribution (for orders linked to cloud_info):")
                print(f"{'Status':<10} | {'Count':<15}")
                print("-" * 30)
                for status, count in results:
                    print(f"{str(status):<10} | {count:<15}")

                # Check if there are cloud_info records without a linked order
                sql_orphan = """
                    SELECT COUNT(*) 
                    FROM cloud_info ci
                    LEFT JOIN `order` o ON ci.order_id = o.order_id
                    WHERE o.id IS NULL AND ci.order_id IS NOT NULL AND ci.order_id != ''
                """
                cur.execute(sql_orphan)
                orphan_count = cur.fetchone()[0]
                print(f"\nCloud_info records with order_id but NO matching order record: {orphan_count}")

        finally:
            conn.close()

if __name__ == "__main__":
    check_all_ci_order_status()
