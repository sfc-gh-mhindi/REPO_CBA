#!/usr/bin/env python3

import os
import re

# Define the path to the DDL extracts folder
ddl_extracts_path = "../../../CBA_GDW_DDL_Extracts"

# Define the dependent table names we need to find (without schema prefix)
dependent_table_names = [
    "GRD_PRTF_TYPE_ATTR",
    "MAP_SAP_INT_GRUP", 
    "TYPE_INT_GRUP",
    "CALENDAR",
    "DIMN_NODE_ASSC",
    "GRD_PRTF_CLAS_ATTR",
    "GRD_PRTF_CATG_ATTR"
]

dependent_views = [
    "PVTECH.GRD_PRTF_TYPE_ENHC"
]

def find_table_ddl_by_name(ddl_files, table_name):
    """Find DDL for a table by name (ignoring schema)"""
    for ddl_file in ddl_files:
        if 'Tables' not in ddl_file:  # Skip view files
            continue
            
        try:
            with open(ddl_file, 'r', encoding='utf-8', errors='ignore') as file:
                content = file.read()
            
            # Look for any schema.TABLE_NAME pattern
            pattern = rf'(/\* <sc-table> \w+\.{re.escape(table_name)} </sc-table> \*/.*?(?=(/\* <sc-table>|$)))'
            match = re.search(pattern, content, re.DOTALL | re.IGNORECASE)
            
            if match:
                # Extract the actual schema.table name from the match
                full_name_match = re.search(rf'<sc-table> (\w+\.{re.escape(table_name)}) </sc-table>', match.group(1))
                if full_name_match:
                    full_name = full_name_match.group(1)
                    print(f"  ✓ Found {table_name} as {full_name} in {os.path.basename(ddl_file)}")
                    return match.group(1).strip(), full_name
                    
        except Exception as e:
            print(f"Error reading {ddl_file}: {e}")
    
    return None, None

def find_view_ddl(ddl_files, view_name):
    """Find DDL for a view"""
    for ddl_file in ddl_files:
        if 'Views' not in ddl_file:  # Skip table files
            continue
            
        try:
            with open(ddl_file, 'r', encoding='utf-8', errors='ignore') as file:
                content = file.read()
            
            # Look for the exact view
            pattern = rf'(/\* <sc-view> {re.escape(view_name)} </sc-view> \*/.*?(?=(/\* <sc-view>|$)))'
            match = re.search(pattern, content, re.DOTALL | re.IGNORECASE)
            
            if match:
                print(f"  ✓ Found {view_name} in {os.path.basename(ddl_file)}")
                return match.group(1).strip()
                    
        except Exception as e:
            print(f"Error reading {ddl_file}: {e}")
    
    return None

def extract_ddls():
    """Extract DDLs for all dependent objects"""
    
    table_ddls = []
    view_ddls = []
    
    # Get list of DDL files
    ddl_files = []
    for file in os.listdir(ddl_extracts_path):
        if file.startswith('DDL_') and file.endswith('.sql'):
            ddl_files.append(os.path.join(ddl_extracts_path, file))
    
    print(f"Found {len(ddl_files)} DDL files to search")
    
    # Search for dependent tables
    print("\n=== SEARCHING FOR DEPENDENT TABLES ===")
    for table_name in dependent_table_names:
        print(f"Searching for {table_name}...")
        
        ddl_content, full_name = find_table_ddl_by_name(ddl_files, table_name)
        if ddl_content:
            table_ddls.append(f"-- {full_name} from DDL extract")
            table_ddls.append(ddl_content)
            table_ddls.append("")  # Add blank line
        else:
            print(f"  ✗ Not found: {table_name}")
    
    # Search for dependent views
    print("\n=== SEARCHING FOR DEPENDENT VIEWS ===")
    for view in dependent_views:
        print(f"Searching for {view}...")
        
        ddl_content = find_view_ddl(ddl_files, view)
        if ddl_content:
            view_ddls.append(f"-- {view} from DDL extract")
            view_ddls.append(ddl_content)
            view_ddls.append("")  # Add blank line
        else:
            print(f"  ✗ Not found: {view}")
    
    # Write table DDLs to tmp6-tbl.sql
    if table_ddls:
        with open('tmp6-tbl.sql', 'w', encoding='utf-8') as f:
            f.write("-- DDLs for tables required by PVTECH.GRD_PRTF_TYPE_ENHC view\n")
            f.write("-- Extracted from CBA_GDW_DDL_Extracts\n\n")
            f.write('\n'.join(table_ddls))
        print(f"\n✓ Written table DDLs to tmp6-tbl.sql")
    
    # Write view DDLs to tmp6-views.sql  
    if view_ddls:
        with open('tmp6-views.sql', 'w', encoding='utf-8') as f:
            f.write("-- DDLs for PVTECH.GRD_PRTF_TYPE_ENHC view\n")
            f.write("-- Extracted from CBA_GDW_DDL_Extracts\n\n")
            f.write('\n'.join(view_ddls))
        print(f"✓ Written view DDLs to tmp6-views.sql")

if __name__ == "__main__":
    extract_ddls() 