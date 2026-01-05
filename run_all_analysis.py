import json
from config import create_ssh_tunnel, get_db_connection
from query_revenue import get_revenue_data
from query_first_period import get_first_period_data
from query_monthly_trials import get_trial_orders_data
from query_cohort_trials import get_cohort_analysis
from query_lag_analysis import get_lag_analysis
from query_registration_device import get_registration_device_analysis
from query_monthly_devices import get_monthly_device_data
from query_business_matrix import get_business_matrix
from query_direct_buyer_history import get_direct_buyer_history
from query_active_subscriptions import get_active_subscriptions_data

def run_all_queries():
    print("Connecting to Remote DB via SSH Tunnel...")
    
    with create_ssh_tunnel() as server:
        conn = get_db_connection()
        try:
            with conn.cursor() as cursor:
                # 1. Revenue & Valid Orders
                revenue_data, valid_orders_data = get_revenue_data(cursor)
                
                # 2. First Period Orders
                first_period_data = get_first_period_data(cursor)
                
                # 3. Monthly Trial Orders
                trial_data = get_trial_orders_data(cursor)
                
                # 4. Cohort Analysis (Unaggregated)
                cohort_data = get_cohort_analysis(cursor)
                
                # 5. Registration Lag
                lag_data = get_lag_analysis(cursor)
                
                # 6. Registration & Device Analysis
                reg_device_data = get_registration_device_analysis(cursor)

                # 7. Monthly New Devices
                device_data = get_monthly_device_data(cursor)

                # 7. Business Performance Matrix
                matrix_data = get_business_matrix(cursor)

                # 8. Direct Buyer History
                history_data = get_direct_buyer_history(cursor)
                
                # 9. Active Subscriptions
                active_sub_data = get_active_subscriptions_data(cursor)

                # Output
                output = {
                    "revenueData": revenue_data,
                    "validOrdersData": valid_orders_data,
                    "firstPeriodData": first_period_data,
                    "trialData": trial_data,
                    "cohortData": cohort_data,
                    "lagData": lag_data,
                    "regDeviceData": reg_device_data,
                    "deviceData": device_data,
                    "businessMatrixData": matrix_data,
                    "buyerHistoryData": history_data,
                    "activeSubscriptionData": active_sub_data
                }
                
                print("\n--- FINAL JSON OUTPUT ---")
                print(json.dumps(output, indent=4))
                
        finally:
            conn.close()

if __name__ == "__main__":
    run_all_queries()
