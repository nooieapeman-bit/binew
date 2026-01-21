
import pymysql
import datetime
from config import create_ssh_tunnel, get_db_connection

# Cycle 4 (4 Weeks Ago): 12/09 - 12/16 (2025)
cycle = {
    "name": "Cycle 4 (4 Weeks Ago): 12/09 - 12/16",
    "start": datetime.datetime(2025, 12, 9, 2, 0, 0, tzinfo=datetime.timezone.utc),
    "end": datetime.datetime(2025, 12, 16, 2, 0, 0, tzinfo=datetime.timezone.utc)
}

with create_ssh_tunnel() as server:
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            start_ts = int(cycle["start"].timestamp())
            end_ts = int(cycle["end"].timestamp())
            
            print(f'\n=== {cycle["name"]} ===')
            
            # 1. Total Registered
            cur.execute(f"SELECT uid, register_time FROM user WHERE register_time >= {start_ts} AND register_time < {end_ts}")
            users = cur.fetchall()
            total_reg = len(users)
            user_reg_map = {r[0]: r[1] for r in users}
            
            if total_reg == 0:
                print("No registrations found.")
                exit()
                
            uids = list(user_reg_map.keys())
            chunk_size = 1000
            
            has_desc_trial = 0
            has_any_trial = 0
            within_24h = 0
            within_60h = 0
            
            for i in range(0, len(uids), chunk_size):
                chunk = uids[i:i+chunk_size]
                placeholders = ', '.join(['%s'] * len(chunk))
                
                # 2. Specific Description
                sql_desc = f"""
                    SELECT count(DISTINCT uid)
                    FROM `order`
                    WHERE uid IN ({placeholders})
                      AND status = 1 
                      AND amount = 0
                      AND description IN ('Trial: 14 DAY', 'Promotion: 14 DAY')
                """
                cur.execute(sql_desc, chunk)
                has_desc_trial += cur.fetchone()[0]
                
                # 3. Any Trial
                sql_any = f"""
                    SELECT uid, MIN(pay_time)
                    FROM `order`
                    WHERE uid IN ({placeholders})
                      AND status = 1
                      AND amount = 0
                    GROUP BY uid
                """
                cur.execute(sql_any, chunk)
                
                for row in cur.fetchall():
                    has_any_trial += 1
                    uid = row[0]
                    pay_time = row[1]
                    if pay_time:
                        reg_time = user_reg_map.get(uid)
                        diff = (pay_time - reg_time) / 3600.0
                        if diff <= 24: within_24h += 1
                        if diff <= 60: within_60h += 1

            print(f'Total Registered: {total_reg}')
            print(f'Trial (Specific Desc): {has_desc_trial} ({(has_desc_trial/total_reg*100):.2f}%)')
            print(f'Trial (Any 0 Order):   {has_any_trial} ({(has_any_trial/total_reg*100):.2f}%)')
            print(f'Within 24h: {within_24h} ({(within_24h/has_any_trial*100) if has_any_trial else 0:.2f}% of trials)')
            print(f'Within 60h: {within_60h} ({(within_60h/has_any_trial*100) if has_any_trial else 0:.2f}% of trials)')

    finally:
        conn.close()
