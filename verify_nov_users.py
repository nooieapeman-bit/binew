import datetime
from config import create_ssh_tunnel, get_db_connection, VALID_PRODUCTS

def verify_users():
    print("Verifying Users for Trial Orders (2025-10-18 ~ 2025-11-16)...")
    
    # Define Window
    start_dt = datetime.datetime(2025, 10, 18, 0, 0, 0)
    end_dt = datetime.datetime(2025, 11, 16, 23, 59, 59)
    start_ts = int(start_dt.timestamp())
    end_ts = int(end_dt.timestamp())
    
    with create_ssh_tunnel() as server:
        conn = get_db_connection()
        try:
            with conn.cursor() as cursor:
                sql = """
                    SELECT o.id, o.uid, u.uid, o.subscribe_id
                    FROM `order` o
                    LEFT JOIN `user` u ON o.uid = u.uid
                    WHERE o.amount = 0 
                      AND o.status = 1 
                      AND o.pay_time >= %s 
                      AND o.pay_time <= %s
                """
                cursor.execute(sql, (start_ts, end_ts))
                rows = cursor.fetchall()
                
                # Logic from query_lag_analysis.py: Deduplicate by subscribe_id
                unique_subs = {}
                for r in rows:
                    sid = r[3]
                    if sid not in unique_subs:
                        unique_subs[sid] = r
                
                total_trials = len(rows) # Raw orders
                dedup_trials = len(unique_subs) # Matches Lag count
                
                missing_users = []
                
                for sid, r in unique_subs.items():
                    order_id = r[0]
                    order_uid = r[1]
                    user_uid = r[2]
                    
                    if not user_uid:
                        missing_users.append({'order_id': order_id, 'uid': order_uid, 'sid': sid})
                
                print(f"Total Raw Orders (No Filter): {total_trials}")
                print(f"Total Deduped SubscribeIDs (Matches Lag Data): {dedup_trials}")
                print(f"Deduped Records with Valid User: {dedup_trials - len(missing_users)}")
                print(f"Deduped Records with Missing User: {len(missing_users)}")
                
                if missing_users:
                    print("\nSample Missing User Orders:")
                    for m in missing_users[:5]:
                        print(m)
        finally:
            conn.close()

if __name__ == "__main__":
    verify_users()
