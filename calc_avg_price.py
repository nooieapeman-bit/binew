
# Based on previous outputs:
# Distribution:
# '14-day video history event recording annually': 31, Price: 29.00
# '14-day history event recording monthly': 29, Price: 3.00
# '30-day video history event recording annually': 22, Price: 49.00
# '30-day video history event recording monthly': 19, Price: 6.00
# '7-day video history CVR recording monthly': 11, Price: 6.00
# '30-day video history event recording AI monthly': 9, Price: 9.00
# '30-day video history event recording pro monthly': 8, Price: 8.00
# '7-day video history CVR recording annually': 8, Price: 49.00
# '14-day video history CVR recording monthly': 6, Price: 9.00
# '30-day video history event recording AI annually': 6, Price: 89.00
# '30-day video history event recording pro annually': 5, Price: 69.00
# '14-day video history CVR recording annually': 4, Price: 89.00
# '14-day video history CVR recording AI monthly': 3, Price: 12.00
# '14-day video history CVR recording pro monthly': 3, Price: 10.00
# '14-day video history CVR recording AI annually': 1, Price: 125.00

# Total Count: 165

data = [
    (31, 29.00),
    (29, 3.00),
    (22, 49.00),
    (19, 6.00),
    (11, 6.00),
    (9, 9.00),
    (8, 8.00),
    (8, 49.00),
    (6, 9.00),
    (6, 89.00),
    (5, 69.00),
    (4, 89.00),
    (3, 12.00),
    (3, 10.00),
    (1, 125.00)
]

total_val = sum(count * price for count, price in data)
total_count = sum(count for count, _ in data)
avg_price = total_val / total_count

print(f"Total Value: {total_val}")
print(f"Total Count: {total_count}")
print(f"Weighted Average Price: {avg_price:.2f} EUR")
