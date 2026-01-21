
import pymysql
from config import create_ssh_tunnel, get_db_connection

pkg_names_a = ['Basic monthly', 'Basic yearly', 'Plus monthly', 'Plus yearly', 'Enhanced monthly', 'Enhanced yearly']
pkg_names_b = ['Basic monthly', 'Basic yearly', 'Platinum monthly', 'Platinum yearly', 'Enhanced monthly', 'Enhanced yearly']

with create_ssh_tunnel() as server:
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # Get Prices for all focused packages in EUR
            all_pkgs = set(pkg_names_a + pkg_names_b)
            placeholders = ', '.join(['%s'] * len(all_pkgs))
            
            sql = f"""
                SELECT sm.name, smp.price
                FROM set_meal_price smp
                JOIN set_meal sm ON smp.set_meal_code = sm.code
                WHERE sm.name IN ({placeholders})
                  AND smp.currency = 'EUR'
            """
            cur.execute(sql, list(all_pkgs))
            price_map = {r[0]: float(r[1]) for r in cur.fetchall()}
            
            print("Prices (EUR):")
            for k, v in price_map.items():
                print(f"{k}: {v}")

            # Calculate Group A Avg
            counts_a = {
                'Basic monthly': 75,
                'Basic yearly': 53,
                'Plus monthly': 8,
                'Plus yearly': 7,
                'Enhanced monthly': 6,
                'Enhanced yearly': 2
            }
            
            total_val_a = 0
            total_cnt_a = 0
            for name, cnt in counts_a.items():
                if name in price_map:
                    total_val_a += cnt * price_map[name]
                    total_cnt_a += cnt
            
            avg_a = total_val_a / total_cnt_a if total_cnt_a else 0
            print(f"\nGroup A Average Trial Price: {avg_a:.2f} EUR")
            
            # Calculate Group B Avg
            counts_b = {
                'Basic monthly': 60,
                'Basic yearly': 50,
                'Platinum monthly': 8,
                'Platinum yearly': 5,
                'Enhanced monthly': 2,
                'Enhanced yearly': 2
            }
            
            total_val_b = 0
            total_cnt_b = 0
            for name, cnt in counts_b.items():
                if name in price_map:
                    total_val_b += cnt * price_map[name]
                    total_cnt_b += cnt
            
            avg_b = total_val_b / total_cnt_b if total_cnt_b else 0
            print(f"Group B Average Trial Price: {avg_b:.2f} EUR")

    finally:
        conn.close()
