import datetime
from config import create_ssh_tunnel, get_db_connection

def check_recent_relationships():
    ts_cutoff = int(datetime.datetime(2024, 10, 1).timestamp())
    print(f"Checking orders submitted after 2024-10-01 (TS >= {ts_cutoff})...")
    
    with create_ssh_tunnel() as server:
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                # 1. Total count of 1:N orders after cutoff
                sql_count = """
                    SELECT COUNT(*) FROM (
                        SELECT o.order_id 
                        FROM `order` o
                        JOIN cloud_info ci ON o.order_id = ci.order_id
                        WHERE o.submit_time >= %s AND ci.is_delete = 0
                        GROUP BY o.order_id 
                        HAVING COUNT(*) > 1
                    ) AS t
                """
                cur.execute(sql_count, (ts_cutoff,))
                total = cur.fetchone()[0]
                print(f"Total orders with multiple cloud_info records: {total}")

                if total > 0:
                    # 2. Detail of samples
                    sql_samples = """
                        SELECT o.order_id, o.submit_time, COUNT(*) 
                        FROM `order` o
                        JOIN cloud_info ci ON o.order_id = ci.order_id
                        WHERE o.submit_time >= %s AND ci.is_delete = 0
                        GROUP BY o.order_id 
                        HAVING COUNT(*) > 1 
                        ORDER BY o.submit_time DESC
                        LIMIT 3
                    """
                    cur.execute(sql_samples, (ts_cutoff,))
                    samples = cur.fetchall()
                    
                    for row in samples:
                        oid, s_time, cnt = row
                        print(f"\nOrder ID: {oid} | Submitted: {datetime.datetime.fromtimestamp(s_time)} | Records: {cnt}")
                        
                        cur.execute("""
                            SELECT id, start_time, end_time, is_event, is_delete 
                            FROM cloud_info 
                            WHERE order_id = %s AND is_delete = 0
                        """, (oid,))
                        for c_row in cur.fetchall():
                            id_val, st, et, ev, dl = c_row
                            print(f"  -> cloud_info ID: {id_val}, Period: {st} to {et}, is_event: {ev}")
        finally:
            conn.close()

if __name__ == "__main__":
    check_recent_relationships()
