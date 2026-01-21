
import pymysql
import datetime
from config import create_ssh_tunnel, get_db_connection

with create_ssh_tunnel() as server:
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # The 5 users identified previously
            uids = [
                'eul1s9r0s1l0t3s9', 
                'eul3m7m6s1l1c0l5', 
                'eul6s3a1s1l2f1q9', 
                'eul7w1m0q1l1r8w5', 
                'eul7l2r1a1l1r1a9'
            ]
            
            print(f'Checking paid subscription status for {len(uids)} converted users...')
            
            placeholders = ', '.join(['%s'] * len(uids))
            
            # 1. Get Paid Orders -> Subscribe ID
            sql = f"""
                SELECT o.uid, o.pay_time, s.name, o.subscribe_id, o.amount
                FROM `order` o
                JOIN set_meal s ON o.product_id = s.code
                WHERE o.uid IN ({placeholders})
                  AND o.amount > 0
                  AND o.status = 1
                ORDER BY o.uid, o.pay_time
            """
            cur.execute(sql, uids)
            paid_rows = cur.fetchall()
            
            sub_ids_to_check = set([r[3] for r in paid_rows])
            
            # 2. Check cancellation status of these subscribe_ids
            sub_status_map = {}
            if sub_ids_to_check:
                s_ph = ', '.join(['%s'] * len(sub_ids_to_check))
                s_list = list(sub_ids_to_check)
                # Ensure we capture max cancel_time if multiple rows exist (though unlikely for single sub_id)
                # Actually, if any row has cancel_time, it's cancelled usually?
                # Let's get the standard check: id desc limit 1 logic via max(cancel_time)
                cur.execute(f"SELECT subscribe_id, MAX(cancel_time) FROM subscribe WHERE subscribe_id IN ({s_ph}) GROUP BY subscribe_id", s_list)
                for r in cur.fetchall():
                    sub_status_map[r[0]] = r[1]
            
            # Print report
            curr_uid = None
            for row in paid_rows:
                uid, pt, pname, sid, amt = row
                pt_dt = datetime.datetime.fromtimestamp(pt)
                
                cancel_ts = sub_status_map.get(sid)
                status_str = f"[CANCELLED at {datetime.datetime.fromtimestamp(cancel_ts)}]" if (cancel_ts and cancel_ts > 0) else "[ACTIVE]"
                
                if uid != curr_uid:
                    print(f'\n=== User {uid} ===')
                    curr_uid = uid
                
                print(f'- Paid Order: {pt_dt} | {pname:<16} | Amt: {amt} | SubID: {sid} {status_str}')

    finally:
        conn.close()
