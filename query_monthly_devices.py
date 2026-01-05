import datetime
import calendar

def get_monthly_device_data(cursor):
    print("Calculating Monthly New Devices...")
    device_data = []
    
    for month_idx in range(1, 13):
        # Time Window for Device Creation (UTC)
        first_day = datetime.datetime(2025, month_idx, 1)
        last_day_val = calendar.monthrange(2025, month_idx)[1]
        last_day_dt = datetime.datetime(2025, month_idx, last_day_val, 23, 59, 59)
        start_ts = int(first_day.timestamp())
        end_ts = int(last_day_dt.timestamp())
        month_str = f"2025-{month_idx:02d}"
        
        sql = "SELECT count(*) FROM device WHERE create_time >= %s AND create_time <= %s"
        cursor.execute(sql, (start_ts, end_ts))
        count = cursor.fetchone()[0]
        
        device_data.append({
            'month': month_str,
            'newDevices': count
        })
        
    return device_data
