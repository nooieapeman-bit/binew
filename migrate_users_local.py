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

def migrate_all_users(limit=None):
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
                    
                    while True:
                        print(f"Fetching batch: offset {offset}...")
                        with remote_conn.cursor() as remote_cur:
                            # Fetching uid, register_time (timestamp), register_country (string/ID)
                            sql = "SELECT uid, register_time, register_country FROM `user` LIMIT %s OFFSET %s"
                            remote_cur.execute(sql, (batch_size, offset))
                            rows = remote_cur.fetchall()
                        
                        if not rows:
                            break
                        
                        # Mapping to local dim_user table:
                        # uid -> uid
                        # app_id -> 1
                        # region_id -> 1
                        # register_country -> country_id
                        # register_time (timestamp) -> register_time (datetime)
                        
                        insert_sql = """
                        INSERT INTO `dim_user` (app_id, region_id, uid, register_time, country_id)
                        VALUES (%s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE register_time = VALUES(register_time), country_id = VALUES(country_id)
                        """
                        
                        batch_data = []
                        for r_uid, r_time, r_country in rows:
                            # Convert timestamp to datetime string for MySQL datetime column
                            dt_str = None
                            if r_time:
                                dt_str = datetime.datetime.fromtimestamp(r_time).strftime('%Y-%m-%d %H:%M:%S')
                            
                            # Convert country string to int if possible, otherwise default to 0
                            c_id = 0
                            try:
                                if r_country:
                                    c_id = int(r_country)
                            except ValueError:
                                pass
                                
                            batch_data.append((1, 1, r_uid, dt_str, c_id))
                        
                        local_cur.executemany(insert_sql, batch_data)
                        local_conn.commit()
                        
                        total_migrated += len(rows)
                        offset += batch_size
                        
                        if limit and total_migrated >= limit:
                            break
                    
                    print(f"Migration completed. Total users migrated: {total_migrated}")
            finally:
                local_conn.close()
        finally:
            remote_conn.close()

if __name__ == "__main__":
    # Start full migration
    migrate_all_users()
