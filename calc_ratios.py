
# Data Injection
# N_A = 151, N_B = 127, N_C = 165

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
# Assuming full data for Control was printed in Step 713
# Let's use the explicit aggregation from Step 713 Output
# Monthly Keywords: 'monthly'
# Yearly Keywords: 'annually'

counts_c_raw = {
    '14-day video history event recording annually': 31,
    '14-day history event recording monthly': 29,
    '30-day video history event recording annually': 22,
    '30-day video history event recording monthly': 19,
    '7-day video history CVR recording monthly': 11,
    '30-day video history event recording AI monthly': 9,
    '30-day video history event recording pro monthly': 8,
    '7-day video history CVR recording annually': 8,
    '14-day video history CVR recording monthly': 6,
    '30-day video history event recording AI annually': 6,
    '30-day video history event recording pro annually': 5,
    '14-day video history CVR recording annually': 4,
    '14-day video history CVR recording AI monthly': 3,
    '14-day video history CVR recording pro monthly': 3,
    '14-day video history CVR recording AI annually': 1
}

def calc_ratio(group_name, counts):
    m_cnt = 0
    y_cnt = 0
    total = 0
    
    for name, cnt in counts.items():
        is_yearly = 'year' in name.lower() or 'annually' in name.lower()
        if is_yearly:
            y_cnt += cnt
        else:
            m_cnt += cnt
        total += cnt

    m_pct = (m_cnt / total * 100) if total else 0
    y_pct = (y_cnt / total * 100) if total else 0
    
    print(f"\n{group_name}:")
    print(f"Total: {total}")
    print(f"Monthly: {m_cnt} ({m_pct:.1f}%)")
    print(f"Yearly:  {y_cnt} ({y_pct:.1f}%)")

calc_ratio("Group A", counts_a)
calc_ratio("Group B", counts_b)
calc_ratio("Control Group", counts_c_raw)
