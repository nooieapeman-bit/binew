
import pymysql
import datetime
from config import create_ssh_tunnel, get_db_connection

# Define Cycles (Working backwards from 2026-01-06)
# Request says "Previous Cycle": 2026-12-30 ? Wait, today is Jan 2026. 
# User likely meant 2025-12-30. 
# Cycle 1 (Target): 2026-01-06 to 2026-01-13 (Already analyzed)
# Cycle 2 (Previous): 2025-12-30 to 2026-01-06 (7 Days)
# Cycle 3 (Pre-Prev): 2025-12-23 to 2025-12-30 (7 Days)
# Cycle 4 (Pre-Pre-Prev): 2025-12-16 to 2025-12-23 (7 Days)

# The user text says "2026-12-30", but contextually must be "2025-12-30" given strictly previous weeks from Jan 6.
# Adjusting year to 2025 for December dates.

cycles = [
    {
        "name": "Cycle 1 (Previous): 12/30 - 01/06",
        "start": datetime.datetime(2025, 12, 30, 2, 0, 0, tzinfo=datetime.timezone.utc),
        "end": datetime.datetime(2026, 1, 6, 2, 0, 0, tzinfo=datetime.timezone.utc)
    },
    {
        "name": "Cycle 2 (2 Weeks Ago): 12/23 - 12/30",
        "start": datetime.datetime(2025, 12, 23, 2, 0, 0, tzinfo=datetime.timezone.utc),
        "end": datetime.datetime(2025, 12, 30, 2, 0, 0, tzinfo=datetime.timezone.utc)
    },
    {
        "name": "Cycle 3 (3 Weeks Ago): 12/16 - 12/23",
        "start": datetime.datetime(2025, 12, 16, 2, 0, 0, tzinfo=datetime.timezone.utc),
        "end": datetime.datetime(2025, 12, 23, 2, 0, 0, tzinfo=datetime.timezone.utc)
    }
]

with create_ssh_tunnel() as server:
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            for cycle in cycles:
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
                    continue
                    
                uids = list(user_reg_map.keys())
                chunk_size = 1000
                
                # Metrics
                has_desc_trial = 0
                has_any_trial = 0
                within_24h = 0
                within_60h = 0
                
                for i in range(0, len(uids), chunk_size):
                    chunk = uids[i:i+chunk_size]
                    placeholders = ', '.join(['%s'] * len(chunk))
                    
                    # 2. Specific Description ('Trial: 14 DAY', 'Promotion: 14 DAY') & Status=1
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
                    
                    # 3. Any Trial (Amount=0, Status=1) + Timestamps
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
