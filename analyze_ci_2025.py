from config import create_ssh_tunnel, get_db_connection
import datetime

def analyze_ci_2025_rel():
    start_2025 = 1735689600
    end_2025 = 1767225599
    
    print(f"Analyzing cloud_info relationships for end_time in 2025 ({start_2025} - {end_2025})...")
    with create_ssh_tunnel() as server:
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                sql = """
                    SELECT 
                        CASE 
                            WHEN o_counts.cnt IS NULL THEN '1:0'
                            WHEN o_counts.cnt = 1 THEN '1:1'
                            ELSE '1:N'
                        END as rel_type,
                        COUNT(*) as ci_count
                    FROM cloud_info ci
                    LEFT JOIN (
                        SELECT order_id, COUNT(*) as cnt 
                        FROM `order` 
                        GROUP BY order_id
                    ) o_counts ON ci.order_id = o_counts.order_id
                    WHERE ci.end_time >= %s AND ci.end_time <= %s
                      AND ci.order_id IS NOT NULL AND ci.order_id != ''
                    GROUP BY rel_type
                """
                cur.execute(sql, (start_2025, end_2025))
                results = cur.fetchall()
                for row in results:
                    print(f"  {row[0]}: {row[1]} cloud_info records")
        finally:
            conn.close()

if __name__ == "__main__":
    analyze_ci_2025_rel()
