#!/usr/bin/env python3

import re

def fix_getbit_calls():
    """Fix all GETBIT() calls in the SQL file"""
    
    print("Reading tmp5-SF Views.sql...")
    
    with open('tmp5-SF Views.sql', 'r') as f:
        content = f.read()
    
    print("Fixing GETBIT() calls...")
    
    # Pattern to match the old GETBIT call format
    old_pattern = r'GETBIT\( \(SELECT ROW_SECU_PRFL_C\s+FROM ([^\s]+)\.PVSECURITY\.ROW_LEVL_SECU_USER_PRFL\s+WHERE UPPER\(RTRIM\(username\)\) = UPPER\(RTRIM\(CURRENT_USER\)\)\s+\),ROW_SECU_ACCS_C\s+\) =1'
    
    # New GETBIT call format
    new_pattern = r'''GETBIT( (
        SELECT
                    try_to_number(ROW_SECU_PRFL_C::varchar)  --Need to double check this logic
        FROM
                    \1.PVSECURITY.ROW_LEVL_SECU_USER_PRFL
        WHERE
                    UPPER(RTRIM( username)) = UPPER(RTRIM(CURRENT_USER))
        ),ROW_SECU_ACCS_C
        ) = 1'''
    
    # Perform the replacement
    updated_content = re.sub(old_pattern, new_pattern, content, flags=re.MULTILINE | re.DOTALL)
    
    # Count replacements
    old_count = len(re.findall(old_pattern, content, flags=re.MULTILINE | re.DOTALL))
    
    # Also handle any variations with P_P01_PVSECURITY
    old_pattern2 = r'GETBIT\( \(SELECT ROW_SECU_PRFL_C\s+FROM ([^\s]+)\.P_P01_PVSECURITY\.ROW_LEVL_SECU_USER_PRFL\s+WHERE UPPER\(RTRIM\(username\)\) = UPPER\(RTRIM\(CURRENT_USER\)\)\s+\),ROW_SECU_ACCS_C\s+\) =1'
    
    new_pattern2 = r'''GETBIT( (
        SELECT
                    try_to_number(ROW_SECU_PRFL_C::varchar)  --Need to double check this logic
        FROM
                    \1.P_P01_PVSECURITY.ROW_LEVL_SECU_USER_PRFL
        WHERE
                    UPPER(RTRIM( username)) = UPPER(RTRIM(CURRENT_USER))
        ),ROW_SECU_ACCS_C
        ) = 1'''
    
    updated_content = re.sub(old_pattern2, new_pattern2, updated_content, flags=re.MULTILINE | re.DOTALL)
    old_count2 = len(re.findall(old_pattern2, content, flags=re.MULTILINE | re.DOTALL))
    
    # Write the updated content
    with open('tmp5-SF Views.sql', 'w') as f:
        f.write(updated_content)
    
    total_replacements = old_count + old_count2
    print(f"✅ Fixed {total_replacements} GETBIT() calls")
    print(f"📁 Updated file: tmp5-SF Views.sql")

if __name__ == "__main__":
    fix_getbit_calls() 