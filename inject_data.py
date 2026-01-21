import json
import re

# Load the fresh data
with open('data_for_js.json', 'r') as f:
    data = json.load(f)

# The file to update
main_js_path = 'bi-dashboard/src/main.js'

with open(main_js_path, 'r') as f:
    content = f.read()

# Define the replacements
replacements = {
    'const revenueData = ': data['revenueData'],
    'const trialData = ': data['trialData'],
    'const validOrdersData = ': data['validOrdersData'],
    'const firstPeriodData = ': data['firstPeriodData'],
    'const cohortData = ': data['cohortData'],
    'const lagData = ': data['lagData'],
    'const regDeviceData = ': data['regDeviceData'],
    'const businessMatrixData = ': data['businessMatrixData'],
    'const deviceData = ': data['deviceData'],
    'const buyerHistoryData = ': data['buyerHistoryData'],
    'const activeSubscriptionData = ': data['activeSubscriptionData'],
    'const detailedActiveData = ': data['detailedActiveData'],
    'const retentionData = ': data['retentionData'],
    'const renewalPeriodData = ': data['renewalPeriodData'],
    'const firstPeriodRegDist = ': data['firstPeriodRegDist']
}

for var_prefix, var_data in replacements.items():
    # Construct searching pattern: from prefix to the next ]; or ];\n or something similar
    # We'll use a regex that handles common JS array/object assignments
    # Pattern: var_prefix = [ ... ]; or var_prefix = [ ... ]
    
    # Escape prefix for regex
    escaped_prefix = re.escape(var_prefix)
    
    # We want to match balanced brackets, but we can approximate by matching until the next line that starts with 'const' or 'let' or end of file
    # Or more simply, match the assignment and its current value.
    # Since we know the structure is "const name = [...];"
    
    pattern = rf'{escaped_prefix}\[.*?\];'
    # Use re.DOTALL to match across lines
    new_val = f"{var_prefix}{json.dumps(var_data, indent=4)};"
    
    if re.search(pattern, content, re.DOTALL):
        content = re.sub(pattern, new_val, content, flags=re.DOTALL)
    else:
        # Try without semicolon
        pattern_no_semi = rf'{escaped_prefix}\[.*?\]'
        if re.search(pattern_no_semi, content, re.DOTALL):
            content = re.sub(pattern_no_semi, new_val.strip(';'), content, flags=re.DOTALL)
        else:
            print(f"Warning: Could not find definition for {var_prefix}")

# Write back the updated file
with open(main_js_path, 'w') as f:
    f.write(content)

print("Successfully updated bi-dashboard/src/main.js with new data.")
