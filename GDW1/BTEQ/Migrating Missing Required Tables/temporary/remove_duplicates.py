#!/usr/bin/env python3

import re

def remove_duplicates():
    """Remove duplicate table creations and their corresponding DROP statements"""
    
    # Read the file
    with open('tmp5-ibrgTbl.sql', 'r') as f:
        content = f.read()
    
    lines = content.split('\n')
    
    # Track seen table names and their positions
    seen_creates = set()
    seen_drops = set()
    lines_to_remove = set()
    
    # First pass: identify duplicate CREATE statements
    for i, line in enumerate(lines):
        if line.strip().startswith('CREATE ICEBERG TABLE'):
            # Extract table name
            match = re.search(r'CREATE ICEBERG TABLE (PS_CLD_RW\.[A-Z_0-9]+\.[A-Z_0-9]+)', line)
            if match:
                table_name = match.group(1)
                if table_name in seen_creates:
                    # This is a duplicate, mark for removal
                    print(f"Found duplicate CREATE for: {table_name}")
                    # Mark the entire table definition for removal
                    j = i
                    while j < len(lines) and not (lines[j].strip().endswith(');') and lines[j].strip() != ');'):
                        lines_to_remove.add(j)
                        j += 1
                    if j < len(lines):
                        lines_to_remove.add(j)  # Include the closing line
                    
                    # Also find and mark the corresponding comment lines
                    k = i - 1
                    while k >= 0 and (lines[k].strip().startswith('/*') or 
                                     lines[k].strip().startswith('--') or 
                                     lines[k].strip() == ''):
                        lines_to_remove.add(k)
                        k -= 1
                else:
                    seen_creates.add(table_name)
    
    # Second pass: identify duplicate DROP statements
    for i, line in enumerate(lines):
        if line.strip().startswith('DROP TABLE IF EXISTS'):
            # Extract table name
            match = re.search(r'DROP TABLE IF EXISTS (PS_CLD_RW\.[A-Z_0-9]+\.[A-Z_0-9]+);', line)
            if match:
                table_name = match.group(1)
                if table_name in seen_drops:
                    # This is a duplicate DROP, mark for removal
                    print(f"Found duplicate DROP for: {table_name}")
                    lines_to_remove.add(i)
                else:
                    seen_drops.add(table_name)
    
    # Filter out the lines to remove
    new_lines = []
    for i, line in enumerate(lines):
        if i not in lines_to_remove:
            new_lines.append(line)
    
    # Write the cleaned content
    with open('tmp5-ibrgTbl.sql', 'w') as f:
        f.write('\n'.join(new_lines))
    
    print(f"Removed {len(lines_to_remove)} lines")
    print(f"Original file: {len(lines)} lines")
    print(f"New file: {len(new_lines)} lines")

if __name__ == "__main__":
    remove_duplicates() 