
# Injection Data
# Basic monthly: 4.99
# Basic yearly:  49.99

counts_a = {
    'Basic monthly': 75, 'Basic yearly': 53,
    'Plus monthly': 8, 'Plus yearly': 7,
    'Enhanced monthly': 6, 'Enhanced yearly': 2
}
counts_b = {
    'Basic monthly': 60, 'Basic yearly': 50,
    'Platinum monthly': 8, 'Platinum yearly': 5,
    'Enhanced monthly': 2, 'Enhanced yearly': 2
}

prices_sim_499 = {
    'Basic monthly': 4.99,   # NEW
    'Basic yearly': 49.99,   # NEW
    'Plus monthly': 8.99, 'Plus yearly': 85.99,
    'Platinum monthly': 10.99, 'Platinum yearly': 109.99,
    'Enhanced monthly': 5.99, 'Enhanced yearly': 59.99
}

def calc_sub_avg_sim(desc, counts, prices_map):
    m_val, m_cnt = 0, 0
    y_val, y_cnt = 0, 0
    
    for name, cnt in counts.items():
        price = prices_map.get(name)
        if not price: continue
        
        is_yearly = 'year' in name.lower() or 'annually' in name.lower()
        if is_yearly:
            y_val += cnt * price
            y_cnt += cnt
        else:
            m_val += cnt * price
            m_cnt += cnt
            
    m_avg = m_val / m_cnt if m_cnt else 0
    y_avg = y_val / y_cnt if y_cnt else 0
    
    print(f"\n{desc} (Simulated 4.99/49.99):")
    print(f"  Monthly Avg: {m_avg:.2f} EUR")
    print(f"  Yearly Avg:  {y_avg:.2f} EUR")

calc_sub_avg_sim("Group A", counts_a, prices_sim_499)
calc_sub_avg_sim("Group B", counts_b, prices_sim_499)
