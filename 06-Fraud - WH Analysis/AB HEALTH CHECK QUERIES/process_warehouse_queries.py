#!/usr/bin/env python3
"""
Process warehouse queries to filter for specific FRAUMD warehouses
and create filtered versions with setup statements.
"""

import os
import re
import glob
from pathlib import Path

# Target warehouses to filter for
TARGET_WAREHOUSES = [
    'WH_USR_PRD_P01_FRAUMD_001',
    'WH_USR_PRD_P01_FRAUMD_LABMLFRD_001', 
    'WH_USR_PRD_P01_FRAUMD_LABMLFRD_002',
    'WH_USR_PRD_P01_FRAUMD_LABMLFRD_003'
]

# Setup statements to add at the beginning
SETUP_STATEMENTS = """-- FRAUMD Warehouse Analysis Queries
-- Filtered for specific FRAUMD warehouses
call pst.svcs.sp_set_account_context (50973,'AU');
USE DATABASE PST;
USE SCHEMA PST.SVCS;

"""

def find_warehouse_filters_in_query(content):
    """Find existing warehouse filters in the query"""
    # Look for common warehouse filtering patterns
    patterns = [
        r"warehouse_name\s*=\s*'([^']+)'",
        r"warehouse_name\s*in\s*\([^)]+\)",
        r"wh\.warehouse_name\s*=\s*'([^']+)'",
        r"wh\.warehouse_name\s*in\s*\([^)]+\)",
        r"w\.warehouse_name\s*=\s*'([^']+)'",
        r"w\.warehouse_name\s*in\s*\([^)]+\)",
        r"WAREHOUSE_NAME\s*=\s*'([^']+)'",
        r"WAREHOUSE_NAME\s*in\s*\([^)]+\)"
    ]
    
    existing_filters = []
    for pattern in patterns:
        matches = re.finditer(pattern, content, re.IGNORECASE)
        for match in matches:
            existing_filters.append(match.group(0))
    
    return existing_filters

def add_warehouse_filter(content, filename):
    """Add or modify warehouse filter in the query"""
    
    # Create the warehouse filter clause
    warehouse_list = "', '".join(TARGET_WAREHOUSES)
    new_filter = f"warehouse_name IN ('{warehouse_list}')"
    
    # Check if there's already a WHERE clause
    if re.search(r'\bWHERE\b', content, re.IGNORECASE):
        # Find existing warehouse filters and replace them
        existing_filters = find_warehouse_filters_in_query(content)
        
        if existing_filters:
            # Replace the first warehouse filter found
            for filter_clause in existing_filters:
                content = content.replace(filter_clause, new_filter)
                break
        else:
            # Add warehouse filter to existing WHERE clause
            # Find the WHERE clause and add our filter
            where_pattern = r'(\bWHERE\b)'
            replacement = f'\\1 {new_filter} AND'
            content = re.sub(where_pattern, replacement, content, count=1, flags=re.IGNORECASE)
    else:
        # No WHERE clause exists, need to add one
        # Try to find a good place to insert WHERE clause
        # Look for common SQL patterns like GROUP BY, ORDER BY, LIMIT
        insert_patterns = [
            r'(\bGROUP\s+BY\b)',
            r'(\bORDER\s+BY\b)', 
            r'(\bLIMIT\b)',
            r'(\bHAVING\b)',
            r'(;?\s*$)'  # End of query
        ]
        
        inserted = False
        for pattern in insert_patterns:
            if re.search(pattern, content, re.IGNORECASE):
                replacement = f'WHERE {new_filter}\n\\1'
                content = re.sub(pattern, replacement, content, count=1, flags=re.IGNORECASE)
                inserted = True
                break
        
        if not inserted:
            # Fallback: add at the end before semicolon or end of file
            if content.rstrip().endswith(';'):
                content = content.rstrip()[:-1] + f'\nWHERE {new_filter};'
            else:
                content = content.rstrip() + f'\nWHERE {new_filter}'
    
    return content

def process_sql_file(input_path, output_dir):
    """Process a single SQL file"""
    filename = os.path.basename(input_path)
    
    # Skip the YAML file
    if filename.endswith('.yml'):
        return None, None
        
    print(f"Processing: {filename}")
    
    try:
        with open(input_path, 'r', encoding='utf-8') as f:
            content = f.read()
    except Exception as e:
        print(f"Error reading {filename}: {e}")
        return None, None
    
    # Add setup statements at the beginning
    processed_content = SETUP_STATEMENTS + "\n" + content
    
    # Add warehouse filter
    try:
        processed_content = add_warehouse_filter(processed_content, filename)
    except Exception as e:
        print(f"Warning: Could not add warehouse filter to {filename}: {e}")
        # Continue with just the setup statements
    
    # Write individual file
    output_path = os.path.join(output_dir, f"fraumd_{filename}")
    try:
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(processed_content)
        print(f"Created: {output_path}")
        return filename, processed_content
    except Exception as e:
        print(f"Error writing {output_path}: {e}")
        return None, None

def main():
    """Main processing function"""
    input_dir = "sql/warehouse"
    output_dir = "AB HEALTH CHECK QUERIES"
    
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    # Get all SQL files
    sql_files = glob.glob(os.path.join(input_dir, "*.sql"))
    sql_files.sort()
    
    print(f"Found {len(sql_files)} SQL files to process")
    print(f"Target warehouses: {TARGET_WAREHOUSES}")
    print(f"Output directory: {output_dir}")
    print("-" * 60)
    
    # Process each file and collect content for collated version
    collated_content = []
    collated_content.append("-- =" * 50)
    collated_content.append("-- FRAUMD WAREHOUSE ANALYSIS - COLLATED QUERIES")
    collated_content.append("-- =" * 50)
    collated_content.append("-- Filtered for warehouses:")
    for wh in TARGET_WAREHOUSES:
        collated_content.append(f"--   {wh}")
    collated_content.append("-- " + "=" * 100)
    collated_content.append("")
    collated_content.append(SETUP_STATEMENTS)
    
    processed_count = 0
    
    for sql_file in sql_files:
        filename, content = process_sql_file(sql_file, output_dir)
        
        if content:
            processed_count += 1
            
            # Add to collated version
            collated_content.append(f"-- " + "=" * 80)
            collated_content.append(f"-- QUERY: {filename}")
            collated_content.append(f"-- " + "=" * 80)
            collated_content.append("")
            
            # Remove the setup statements from individual content since we have them at the top
            content_without_setup = content.replace(SETUP_STATEMENTS, "").strip()
            collated_content.append(content_without_setup)
            collated_content.append("")
            collated_content.append("")
    
    # Write collated file
    collated_path = os.path.join(output_dir, "fraumd_all_warehouse_queries.sql")
    try:
        with open(collated_path, 'w', encoding='utf-8') as f:
            f.write("\n".join(collated_content))
        print("-" * 60)
        print(f"✅ Created collated file: {collated_path}")
        print(f"✅ Processed {processed_count} SQL files successfully")
        
        # Show file sizes
        print("\nCreated files:")
        for file in sorted(glob.glob(os.path.join(output_dir, "*.sql"))):
            size = os.path.getsize(file)
            print(f"  {os.path.basename(file)}: {size:,} bytes")
            
    except Exception as e:
        print(f"❌ Error creating collated file: {e}")

if __name__ == "__main__":
    main() 