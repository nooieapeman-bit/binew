
import pymysql
import datetime
from config import create_ssh_tunnel, get_db_connection

# Timestamps (UTC)
start_dt = datetime.datetime(2026, 1, 6, 2, 0, 0, tzinfo=datetime.timezone.utc)
end_dt = datetime.datetime(2026, 1, 13, 2, 0, 0, tzinfo=datetime.timezone.utc)
start_ts = int(start_dt.timestamp())
end_ts = int(end_dt.timestamp())

platinum_codes = set([
    'c22f95e0eb3856e083ab265a97b5be9f', 
    '50e5b771de60f1816e964a7ef097f120'
])

with create_ssh_tunnel() as server:
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # 1. Fetch Group B UIDs
            cur.execute(f"""
                SELECT u.uid, sc.set_meal_code
                FROM user u
                JOIN set_meal_user_rule sc ON u.uid = sc.uid
                WHERE u.register_time >= {start_ts} AND u.register_time < {end_ts}
            """)
            user_codes = {}
            for row in cur.fetchall():
                uid, code = row
                if uid not in user_codes: user_codes[uid] = set()
                user_codes[uid].add(code)
            
            group_b_uids = [] 
            for uid, codes in user_codes.items():
                if not codes.isdisjoint(platinum_codes):
                    group_b_uids.append(uid)
            
            # 2. Find Cancelled Standard Trials
            chunk_size = 1000
            cancelled_details = []
            
            for i in range(0, len(group_b_uids), chunk_size):
                chunk = group_b_uids[i:i+chunk_size]
                placeholders = ', '.join(['%s'] * len(chunk))
                
                sql = f"""
                    SELECT DISTINCT subscribe_id, uid, s.name, o.pay_time
                    FROM `order` o
                    JOIN set_meal s ON o.product_id = s.code
                    WHERE o.uid IN ({placeholders})
                      AND o.amount = 0
                      AND o.status = 1
                      AND o.description IN ('Trial: 14 DAY', 'Promotion: 14 DAY')
                      AND o.subscribe_id != ''
                """
                cur.execute(sql, chunk)
                rows = cur.fetchall()
                
                if not rows: continue
                
                sub_detail_map = {r[0]: {'uid': r[1], 'pname': r[2], 'pt': r[3]} for r in rows}
                sids = list(sub_detail_map.keys())
                
                sph = ', '.join(['%s'] * len(sids))
                sql_check = f"SELECT subscribe_id, MAX(cancel_time) FROM subscribe WHERE subscribe_id IN ({sph}) GROUP BY subscribe_id"
                cur.execute(sql_check, sids)
                
                for r in cur.fetchall():
                    sid = r[0]
                    ctime = r[1]
                    if ctime and ctime > 0:
                        d = sub_detail_map[sid]
                        d['sid'] = sid
                        d['cancel_time'] = ctime
                        cancelled_details.append(d)
            
            print(f'Found {len(cancelled_details)} cancelled trials in Group B:')
            
            # Sort by cancel time
            cancelled_details.sort(key=lambda x: x['cancel_time'])
            
            for d in cancelled_details:
                pt_dt = datetime.datetime.fromtimestamp(d['pt'])
                ct_dt = datetime.datetime.fromtimestamp(d['cancel_time'])
                print(f"User: {d['uid']} | Pkg: {d['pname']:<12} | TrialStart: {pt_dt} | Cancelled: {ct_dt}")

    finally:
        conn.close()
