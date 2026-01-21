import pymysql
import datetime

# Local DB Config
LOCAL_DB_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'user': 'root',
    'password': 'ne@202509',
    'database': 'bi'
}

def backup_tables():
    # Current timestamp for suffix
    now = datetime.datetime.now().strftime('%Y%m%d_%H%M')
    
    tables_to_backup = [
        'dim_user',
        'dim_device', 
        'dim_user_device',
        'dim_subscription',
        'fact_order',
        'fact_cloud'
    ]
    
    conn = pymysql.connect(**LOCAL_DB_CONFIG)
    try:
        with conn.cursor() as cur:
            for table in tables_to_backup:
                # Check if table exists
                cur.execute(f"SHOW TABLES LIKE '{table}'")
                if cur.fetchone():
                    new_name = f"{table}_{now}"
                    print(f"Renaming {table} to {new_name}...")
                    cur.execute(f"RENAME TABLE `{table}` TO `{new_name}`")
                    
                    # Create empty table with same schema to prevent errors in subsequent scripts checking for existence
                    # (Optional, but user said 'Before syncing', implied we need clean slate. 
                    # The full sync script will likely assume tables exist or create them. 
                    # Let's just Rename for now as requested.)
                else:
                    print(f"Table {table} does not exist, skipping.")
        conn.commit()
        print("Backup completed successfully.")
    except Exception as e:
        print(f"Error during backup: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    backup_tables()
