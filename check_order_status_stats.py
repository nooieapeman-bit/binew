from config import create_ssh_tunnel, get_db_connection

def check_order_status_stats():
    print("Fetching order status statistics from the remote database...")
    with create_ssh_tunnel() as server:
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                # Use backticks for table name since 'order' is a reserved word
                sql = "SELECT status, COUNT(*) FROM `order` GROUP BY status"
                cur.execute(sql)
                results = cur.fetchall()
                
                total = sum(row[1] for row in results)
                print(f"Total entries in 'order' table: {total}")
                print("-" * 40)
                print(f"{'Status':<10} | {'Count':<15} | {'Percentage':<10}")
                print("-" * 40)
                for status, count in results:
                    percentage = (count / total * 100) if total > 0 else 0
                    print(f"{str(status):<10} | {count:<15} | {percentage:.2f}%")
        finally:
            conn.close()

if __name__ == "__main__":
    check_order_status_stats()
