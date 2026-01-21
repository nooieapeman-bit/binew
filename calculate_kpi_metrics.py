import pymysql
import datetime
import time
import json
from config import LOCAL_BIND_PORT, DB_USER, DB_PASS, DB_NAME

LOCAL_DB_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'user': 'root',
    'password': 'ne@202509',
    'database': 'bi'
}

def get_kpi_metrics():
    today = datetime.date(2026, 1, 8)
    end_date = today 
    start_date = end_date - datetime.timedelta(days=7) 
    
    prev_end_date = start_date
    prev_start_date = prev_end_date - datetime.timedelta(days=7)

    conn = pymysql.connect(**LOCAL_DB_CONFIG)
    cur = conn.cursor(pymysql.cursors.DictCursor)

    def get_stats_for_period(s_date, e_date):
        # 1. New Trials
        cur.execute("SELECT COUNT(*) as count FROM fact_cloud WHERE amount = 0 AND pay_time >= %s AND pay_time < %s", (s_date, e_date))
        trials_count = cur.fetchone()['count']

        # 2. Registrations
        cur.execute("SELECT COUNT(*) as count FROM dim_user WHERE register_time >= %s AND register_time < %s", (s_date, e_date))
        reg_count = cur.fetchone()['count']

        # 3. Bindings
        cur.execute("SELECT COUNT(*) as count FROM dim_user_device WHERE first_bind_time >= %s AND first_bind_time < %s", (s_date, e_date))
        binds_count = cur.fetchone()['count']

        # 4. Cohort Rates (Trial)
        cur.execute("""
            SELECT 
                COUNT(*) as total,
                COUNT(IF(TIMESTAMPDIFF(HOUR, register_time, first_trial_time) <= 24, 1, NULL)) as t24h,
                COUNT(IF(TIMESTAMPDIFF(HOUR, register_time, first_trial_time) <= 96, 1, NULL)) as t4d,
                COUNT(IF(TIMESTAMPDIFF(DAY, register_time, first_trial_time) <= 30, 1, NULL)) as t30d
            FROM dim_user
            WHERE register_time >= %s AND register_time < %s
        """, (s_date, e_date))
        cohort = cur.fetchone()
        
        return {
            'new_trials': trials_count,
            'new_binds': binds_count,
            'rate_24h': (cohort['t24h'] / cohort['total'] * 100) if cohort['total'] > 0 else 0,
            'rate_4d': (cohort['t4d'] / cohort['total'] * 100) if cohort['total'] > 0 else 0,
            'rate_30d': (cohort['t30d'] / cohort['total'] * 100) if cohort['total'] > 0 else 0
        }

    curr_stats = get_stats_for_period(start_date, end_date)
    prev_stats = get_stats_for_period(prev_start_date, prev_end_date)
    conn.close()

    def calc_pop(curr, prev):
        if prev == 0: return 0
        return round((curr - prev) / prev * 100, 1)

    metrics = [
        {
            'label': 'New Trials (7d)',
            'value': f"{curr_stats['new_trials']:,}",
            'pop': calc_pop(curr_stats['new_trials'], prev_stats['new_trials'])
        },
        {
            'label': 'New Devices (7d)',
            'value': f"{curr_stats['new_binds']:,}",
            'pop': calc_pop(curr_stats['new_binds'], prev_stats['new_binds'])
        },
        {
            'label': '24h Trial Rate',
            'value': f"{curr_stats['rate_24h']:.1f}%",
            'pop': calc_pop(curr_stats['rate_24h'], prev_stats['rate_24h'])
        },
        {
            'label': '4d Trial Rate',
            'value': f"{curr_stats['rate_4d']:.1f}%",
            'pop': calc_pop(curr_stats['rate_4d'], prev_stats['rate_4d'])
        },
        {
            'label': '30d Trial Rate',
            'value': f"{curr_stats['rate_30d']:.1f}%",
            'pop': calc_pop(curr_stats['rate_30d'], prev_stats['rate_30d'])
        }
    ]

    print(json.dumps(metrics, indent=4))

if __name__ == "__main__":
    get_kpi_metrics()
