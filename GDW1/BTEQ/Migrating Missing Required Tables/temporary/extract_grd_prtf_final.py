#!/usr/bin/env python3

import os
import re

# Define the path to the DDL extracts folder
ddl_extracts_path = "../../../CBA_GDW_DDL_Extracts"

# Based on the grep search results, these are the most likely candidates
specific_tables = [
    "PDCBSTG.RPC_GRD_PRTF_TYPE_ATTR",  # GRD_PRTF_TYPE_ATTR
    "PDRCTL.MAP_SAP_INT_GRUP",         # MAP_SAP_INT_GRUP
    "PDRCTL.TYPE_INT_GRUP",            # TYPE_INT_GRUP  
    "UDCRCD.REF_CALENDAR",             # CALENDAR (choosing REF_CALENDAR as most generic)
    "PDGRD.DIMN_NODE_ASSC",            # DIMN_NODE_ASSC
    "PDCBSTG.RPC_GRD_PRTF_CATG_ATTR"  # GRD_PRTF_CATG_ATTR
]

# Missing: GRD_PRTF_CLAS_ATTR (not found in any DDL)

specific_views = [
    "PVTECH.GRD_PRTF_TYPE_ENHC"
]

def extract_ddl_for_object(ddl_files, object_name):
    """Extract DDL for a specific object"""
    for ddl_file in ddl_files:
        # Determine if searching for table or view
        is_view = 'Views' in ddl_file
        is_table = 'Tables' in ddl_file
        
        if object_name.count('.') == 1:  # schema.object format
            if ('PVTECH' in object_name and not is_view) or ('PVTECH' not in object_name and not is_table):
                continue
        
        try:
            with open(ddl_file, 'r', encoding='utf-8', errors='ignore') as file:
                content = file.read()
            
            # Search pattern
            if is_view:
                pattern = rf'(/\* <sc-view> {re.escape(object_name)} </sc-view> \*/.*?(?=(/\* <sc-view>|$)))'
            else:
                pattern = rf'(/\* <sc-table> {re.escape(object_name)} </sc-table> \*/.*?(?=(/\* <sc-table>|$)))'
            
            match = re.search(pattern, content, re.DOTALL | re.IGNORECASE)
            
            if match:
                print(f"  ✓ Found {object_name} in {os.path.basename(ddl_file)}")
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
    
    # Search for specific tables
    print("\n=== SEARCHING FOR DEPENDENT TABLES ===")
    for table in specific_tables:
        print(f"Searching for {table}...")
        
        ddl_content = extract_ddl_for_object(ddl_files, table)
        if ddl_content:
            table_ddls.append(f"-- {table} from DDL extract")
            table_ddls.append(ddl_content)
            table_ddls.append("")  # Add blank line
        else:
            print(f"  ✗ Not found: {table}")
    
    # Search for views
    print("\n=== SEARCHING FOR DEPENDENT VIEWS ===")
    for view in specific_views:
        print(f"Searching for {view}...")
        
        ddl_content = extract_ddl_for_object(ddl_files, view)
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
        print(f"\n✓ Written {len(specific_tables)} table DDLs to tmp6-tbl.sql")
    
    # Write view DDLs to tmp6-views.sql  
    if view_ddls:
        with open('tmp6-views.sql', 'w', encoding='utf-8') as f:
            f.write("-- DDLs for PVTECH.GRD_PRTF_TYPE_ENHC view\n")
            f.write("-- Extracted from CBA_GDW_DDL_Extracts\n\n")
            f.write('\n'.join(view_ddls))
        print(f"✓ Written {len(specific_views)} view DDL to tmp6-views.sql")

    print(f"\nNote: GRD_PRTF_CLAS_ATTR was not found in any DDL files.")
    print(f"Total extracted: {len([d for d in table_ddls if d.startswith('--')])} tables + {len([d for d in view_ddls if d.startswith('--')])} view")

if __name__ == "__main__":
    extract_ddls() 