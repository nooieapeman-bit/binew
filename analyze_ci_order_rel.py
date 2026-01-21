from config import create_ssh_tunnel, get_db_connection

def analyze_ci_order_rel():
    print("Connecting to remote database to analyze cloud_info -> order relationship...")
    with create_ssh_tunnel() as server:
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                # 1. First, get a distribution of how many orders exist for each order_id in the 'order' table
                # This helps us understand if order_id is truly unique in the order table.
                print("Step 1: Analyzing order_id frequency in 'order' table...")
                cur.execute("""
                    SELECT order_count, COUNT(*) as unique_order_ids
                    FROM (
                        SELECT order_id, COUNT(*) as order_count 
                        FROM `order` 
                        WHERE order_id IS NOT NULL AND order_id != ''
                        GROUP BY order_id
                    ) t
                    GROUP BY order_count
                """)
                order_freq = cur.fetchall()
                print("Order ID frequency in 'order' table (count of orders with same ID):")
                for count, freq in order_freq:
                    print(f"  {count} order(s): {freq} unique orderIDs")

                # 2. Analyze relationship from cloud_info's perspective
                print("\nStep 2: Analyzing cloud_info -> order relationship...")
                # We'll use a temporary table or a join to count.
                # Since cloud_info has ~500k rows (based on previous logs/context), a join is feasible.
                
                cur.execute("""
                    SELECT 
                        CASE 
                            WHEN o_counts.cnt IS NULL THEN 0 
                            WHEN o_counts.cnt = 1 THEN 1 
                            ELSE 2 -- Representing N
                        END as rel_type,
                        COUNT(*) as ci_row_count
                    FROM cloud_info ci
                    LEFT JOIN (
                        SELECT order_id, COUNT(*) as cnt 
                        FROM `order` 
                        GROUP BY order_id
                    ) o_counts ON ci.order_id = o_counts.order_id
                    WHERE ci.order_id IS NOT NULL AND ci.order_id != ''
                    GROUP BY rel_type
                """)
                results = cur.fetchall()
                
                print("\nCloudInfo -> Order Relationship (Based on order_id):")
                summary = {row[0]: row[1] for row in results}
                print(f"  1 cloud_info : 0 order (1:0): {summary.get(0, 0)}")
                print(f"  1 cloud_info : 1 order (1:1): {summary.get(1, 0)}")
                print(f"  1 cloud_info : N orders (1:N): {summary.get(2, 0)}")

                # 3. Check for NULL/Empty order_id in cloud_info
                cur.execute("SELECT COUNT(*) FROM cloud_info WHERE order_id IS NULL OR order_id = ''")
                empty_ci = cur.fetchone()[0]
                print(f"\nCloudInfo records with NULL or Empty order_id: {empty_ci}")

        finally:
            conn.close()

if __name__ == "__main__":
    analyze_ci_order_rel()
