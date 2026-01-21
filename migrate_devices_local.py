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

def migrate_devices(limit=None):
    print("Connecting to remote database...")
    with create_ssh_tunnel() as server:
        remote_conn = get_db_connection()
        try:
            print(f"Connecting to local database '{LOCAL_DB_CONFIG['database']}'...")
            local_conn = pymysql.connect(**LOCAL_DB_CONFIG)
            try:
                with local_conn.cursor() as local_cur:
                    # Batch Migration
                    batch_size = 2000
                    offset = 0
                    total_migrated = 0
                    
                    if limit and limit < batch_size:
                        batch_size = limit

                    while True:
                        print(f"Fetching batch: offset {offset}...")
                        with remote_conn.cursor() as remote_cur:
                            # remote query: uuid, model_code, create_time (timestamp)
                            sql = "SELECT uuid, model_code, create_time FROM `device` LIMIT %s OFFSET %s"
                            remote_cur.execute(sql, (batch_size, offset))
                            rows = remote_cur.fetchall()
                        
                        if not rows:
                            break
                        
                        # Mapping to local dim_device table:
                        # uuid -> uuid
                        # app_id -> 1
                        # region_id -> 1
                        # model_code -> model_code
                        # create_time (timestamp) -> create_time (datetime)
                        
                        insert_sql = """
                        INSERT INTO `dim_device` (app_id, region_id, uuid, model_code, create_time)
                        VALUES (%s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE model_code = VALUES(model_code), create_time = VALUES(create_time)
                        """
                        
                        batch_data = []
                        for r_uuid, r_model, r_time in rows:
                            dt_str = None
                            if r_time:
                                try:
                                    dt_str = datetime.datetime.fromtimestamp(r_time).strftime('%Y-%m-%d %H:%M:%S')
                                except:
                                    pass
                            
                            batch_data.append((1, 1, r_uuid, r_model if r_model else '', dt_str))
                        
                        local_cur.executemany(insert_sql, batch_data)
                        local_conn.commit()
                        
                        total_migrated += len(rows)
                        offset += batch_size
                        
                        if limit and total_migrated >= limit:
                            break
                    
                    print(f"Migration completed. Total devices migrated: {total_migrated}")
            finally:
                local_conn.close()
        finally:
            remote_conn.close()

if __name__ == "__main__":
    # Start full migration for devices
    migrate_devices()
