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

def migrate_fact_cloud_phase1():
    print("Connecting to remote database via SSH...")
    with create_ssh_tunnel(local_port=13444) as server:
        from config import DB_USER, DB_PASS, DB_NAME
        remote_conn = pymysql.connect(host='127.0.0.1', port=13444, user=DB_USER, password=DB_PASS, database=DB_NAME)
        try:
            print("Connecting to local database...")
            local_conn = pymysql.connect(**LOCAL_DB_CONFIG)
            try:
                with local_conn.cursor() as local_cur:
                    batch_size = 5000
                    offset = 0
                    total_migrated = 0
                    
                    print("Starting Phase 1 migration: Direct mapping from cloud_info...")
                    
                    while True:
                        with remote_conn.cursor(pymysql.cursors.DictCursor) as remote_cur:
                            sql = "SELECT uid, uuid, order_id, start_time, end_time, is_delete FROM `cloud_info` LIMIT %s OFFSET %s"
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
                            INSERT INTO fact_cloud (
                                app_id, region_id, uid, uuid, order_id, 
                                cycle_start_time, cycle_end_time, cycle_is_delete
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        """
                        
                        batch_data = []
                        for r in rows:
                            batch_data.append((
                                1, # app_id
                                1, # region_id
                                r['uid'],
                                r['uuid'],
                                r['order_id'] if r['order_id'] else '',
                                ts_to_dt(r['start_time']),
                                ts_to_dt(r['end_time']),
                                r['is_delete']
                            ))
                        
                        local_cur.executemany(insert_sql, batch_data)
                        local_conn.commit()
                        
                        total_migrated += len(rows)
                        offset += batch_size
                        if (total_migrated // batch_size) % 5 == 0:
                            print(f"Migrated {total_migrated} records...")

                    print(f"Phase 1 Migration completed. Total records: {total_migrated}")
                    
            finally:
                local_conn.close()
        finally:
            remote_conn.close()

if __name__ == "__main__":
    migrate_fact_cloud_phase1()
