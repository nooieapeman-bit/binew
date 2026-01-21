
# Data Injection for EU Region

# Group A (Basic/Plus) - N=151 Trial Subs
data_a = [
    ('Group A', 'Basic monthly', 'Monthly', 2.99, 75),
    ('Group A', 'Basic yearly', 'Yearly', 33.99, 53),
    ('Group A', 'Plus monthly', 'Monthly', 8.99, 8),
    ('Group A', 'Plus yearly', 'Yearly', 85.99, 7),
    ('Group A', 'Enhanced monthly', 'Monthly', 5.99, 6),
    ('Group A', 'Enhanced yearly', 'Yearly', 59.99, 2),
]

# Group B (Basic/Platinum) - N=127 Trial Subs
data_b = [
    ('Group B', 'Basic monthly', 'Monthly', 2.99, 60),
    ('Group B', 'Basic yearly', 'Yearly', 33.99, 50),
    ('Group B', 'Platinum monthly', 'Monthly', 10.99, 8),
    ('Group B', 'Platinum yearly', 'Yearly', 109.99, 5),
    ('Group B', 'Enhanced monthly', 'Monthly', 5.99, 2),
    ('Group B', 'Enhanced yearly', 'Yearly', 59.99, 2),
]

# Control Group - N=165 Trial Subs
data_c = [
    ('Control', '14-day video history event recording annually', 'Yearly', 29.00, 31),
    ('Control', '14-day history event recording monthly', 'Monthly', 3.00, 29),
    ('Control', '30-day video history event recording annually', 'Yearly', 49.00, 22),
    ('Control', '30-day video history event recording monthly', 'Monthly', 6.00, 19),
    ('Control', '7-day video history CVR recording monthly', 'Monthly', 6.00, 11),
    ('Control', '30-day video history event recording AI monthly', 'Monthly', 9.00, 9),
    ('Control', '30-day video history event recording pro monthly', 'Monthly', 8.00, 8),
    ('Control', '7-day video history CVR recording annually', 'Yearly', 49.00, 8),
    ('Control', '14-day video history CVR recording monthly', 'Monthly', 9.00, 6),
    ('Control', '30-day video history event recording AI annually', 'Yearly', 89.00, 6),
    ('Control', '30-day video history event recording pro annually', 'Yearly', 69.00, 5),
    ('Control', '14-day video history CVR recording annually', 'Yearly', 89.00, 4),
    ('Control', '14-day video history CVR recording AI monthly', 'Monthly', 12.00, 3),
    ('Control', '14-day video history CVR recording pro monthly', 'Monthly', 10.00, 3),
    ('Control', '14-day video history CVR recording AI annually', 'Yearly', 125.00, 1),
]

all_data = data_a + data_b + data_c

print("| 组别 | 套餐名称 | 套餐类型 | 套餐价格 (EUR) | 当前试用人数 | LTV 收入 (EUR) |")
print("| :--- | :--- | :--- | :--- | :--- | :--- |")

sums = {
    'Group A': {'Monthly': {'cnt': 0, 'rev': 0}, 'Yearly': {'cnt': 0, 'rev': 0}},
    'Group B': {'Monthly': {'cnt': 0, 'rev': 0}, 'Yearly': {'cnt': 0, 'rev': 0}},
    'Control': {'Monthly': {'cnt': 0, 'rev': 0}, 'Yearly': {'cnt': 0, 'rev': 0}},
}

for row in all_data:
    group, name, ptype, price, count = row
    
    # LTV Logic
    multiplier = 1.75 if ptype == 'Yearly' else 6.7
    revenue = price * count * multiplier
    
    sums[group][ptype]['cnt'] += count
    sums[group][ptype]['rev'] += revenue
    
    nice_name = name.replace('video history event recording', 'Event').replace('history event recording', 'Event')
    print(f"| {group} | {nice_name} | {ptype} | €{price:.2f} | {count} | €{revenue:.2f} |")

print(f"| | | | | |")

# Summaries
for grp in ['Group A', 'Group B', 'Control']:
    m_cnt = sums[grp]['Monthly']['cnt']
    m_rev = sums[grp]['Monthly']['rev']
    y_cnt = sums[grp]['Yearly']['cnt']
    y_rev = sums[grp]['Yearly']['rev']
    total_cnt = m_cnt + y_cnt
    total_rev = m_rev + y_rev
    
    print(f"| **{grp} Monthly** | | | | **{m_cnt}** | **€{m_rev:.2f}** |")
    print(f"| **{grp} Yearly** | | | | **{y_cnt}** | **€{y_rev:.2f}** |")
    print(f"| **{grp} Total** | | | | **{total_cnt}** | **€{total_rev:.2f}** |")
    print(f"| | | | | |")

# Registered Users for reference
# A: 1714, B: 1519, C: 2592 (EU Data)
reg_users = {'Group A': 1714, 'Group B': 1519, 'Control': 2592}

print(f"\n**综合价值 (Revenue per 1,000 Registered Users):**")
for grp in ['Group A', 'Group B', 'Control']:
    total_rev = sums[grp]['Monthly']['rev'] + sums[grp]['Yearly']['rev']
    reg = reg_users[grp]
    rev_per_1000 = (total_rev / reg) * 1000
    print(f"{grp}: €{rev_per_1000:.2f}")
