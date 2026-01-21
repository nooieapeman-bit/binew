
import pymysql
from config import create_ssh_tunnel, get_db_connection

with create_ssh_tunnel() as server:
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # Safer SQL execution
            sql = "SELECT * FROM `order` WHERE uid = %s AND amount=0 AND status=1"
            cur.execute(sql, ('eul0q0r3c1l1t1c4',))
            rows = cur.fetchall()
            
            if not rows:
                print('No rows found')
            else:
                cols = [desc[0] for desc in cur.description]
                print(f'Columns: {cols}')
                
                print(f'\nComparing {len(rows)} rows for variations...')
                
                # Check uniqueness of each column across these rows
                for i, col in enumerate(cols):
                    values = [row[i] for row in rows]
                    unique_values = set(values)
                    
                    if len(unique_values) > 1:
                        print(f'Column {col:<20} VARIES: {len(unique_values)} values')
                        if col == 'order_id' or col == 'id' or len(str(values[0])) < 50:
                            print(f'  Values: {values}')
                    else:
                        # Optional: Print identical values if interesting?
                        pass

    finally:
        conn.close()
