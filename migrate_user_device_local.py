import pymysql
import datetime
from config import create_ssh_tunnel, get_db_connection

# Local DB Config
LOCAL_DB_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'user': 'root',
    'password': 'ne@202509',
    'database': 'bi'
}

def migrate_user_devices(limit=None):
    print("Connecting to remote database...")
    # Use a different port to avoid conflict with potential background processes
    with create_ssh_tunnel(local_port=13312) as server:
        from config import DB_USER, DB_PASS, DB_NAME
        remote_conn = pymysql.connect(host='127.0.0.1', port=13312, user=DB_USER, password=DB_PASS, database=DB_NAME)
        try:
            print(f"Connecting to local database '{LOCAL_DB_CONFIG['database']}'...")
            local_conn = pymysql.connect(**LOCAL_DB_CONFIG)
            try:
                with local_conn.cursor() as local_cur:
                    # Truncate to re-cleanse
                    print("Truncating local table 'dim_user_device'...")
                    local_cur.execute("TRUNCATE TABLE dim_user_device")
                    local_conn.commit()

                    batch_size = 2000
                    offset = 0
                    total_migrated = 0
                    
                    if limit and limit < batch_size:
                        batch_size = limit

                    while True:
                        print(f"Fetching batch: offset {offset}...")
                        with remote_conn.cursor(pymysql.cursors.DictCursor) as remote_cur:
                            sql = "SELECT uid, uuid, model_code, device_type, bind_type, status, first_time, bind_time, delete_time, created_at FROM `user_device` LIMIT %s OFFSET %s"
                            remote_cur.execute(sql, (batch_size, offset))
                            rows = remote_cur.fetchall()
                        
                        if not rows:
                            break
                        
                        def ts_to_dt(ts):
                            if ts and ts > 0:
                                try:
                                    return datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
                                except:
                                    return None
                            return None

                        insert_sql = """
                        INSERT INTO `dim_user_device` (
                            uid, uuid, app_id, region_id, model_code, device_type, bind_type, status, 
                            first_bind_time, last_bind_time, delete_time, create_time
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """
                        
                        batch_data = []
                        for r in rows:
                            batch_data.append((
                                r['uid'],
                                r['uuid'],
                                1, # app_id
                                1, # region_id
                                r['model_code'] if r['model_code'] else '',
                                r['device_type'] if r['device_type'] is not None else 0,
                                r['bind_type'] if r['bind_type'] is not None else 1,
                                r['status'] if r['status'] is not None else 1,
                                ts_to_dt(r['first_time']),
                                ts_to_dt(r['bind_time']),
                                ts_to_dt(r['delete_time']),
                                r['created_at'] # created_at is timestamp, so pymysql returns datetime
                            ))
                        
                        local_cur.executemany(insert_sql, batch_data)
                        local_conn.commit()
                        
                        total_migrated += len(rows)
                        offset += batch_size
                        
                        if limit and total_migrated >= limit:
                            break
                    
                    print(f"Migration completed. Total records migrated: {total_migrated}")
            finally:
                local_conn.close()
        finally:
            remote_conn.close()

if __name__ == "__main__":
    # Full migration
    migrate_user_devices(limit=None)
