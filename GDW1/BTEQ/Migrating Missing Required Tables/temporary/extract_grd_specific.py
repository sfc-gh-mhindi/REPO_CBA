#!/usr/bin/env python3

import json
import re
import os
from collections import defaultdict

def load_missing_objects():
    """Load missing tables/views from the JSON file"""
    with open('referenced_forth_wave_updated.json', 'r') as f:
        data = json.load(f)
    return data['missing_tables']

def search_object_in_ddl_files(object_name, ddl_folder):
    """Search for object by name (ignoring schema) in all DDL files"""
    print(f"  Searching for object: {object_name}")
    
    ddl_files = [
        'DDL_Tables.sql',
        'DDL_Tables1.sql', 
        'DDL_Tables2.sql',
        'DDL_Views.sql',
        'DDL_Views1.sql',
        'DDL_Views2.sql'
    ]
    
    found_objects = []
    
    for ddl_file in ddl_files:
        file_path = os.path.join(ddl_folder, ddl_file)
        if not os.path.exists(file_path):
            continue
            
        print(f"    Searching in {ddl_file}...")
        
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
            
            # Look for comment patterns first (more reliable)
            table_comment_pattern = rf'/\*\s*<sc-table>\s+([A-Z_][A-Z0-9_]*\.[A-Z_][A-Z0-9_]*)\s*</sc-table>\s*\*/'
            view_comment_pattern = rf'/\*\s*<sc-view>\s+([A-Z_][A-Z0-9_]*\.[A-Z_][A-Z0-9_]*)\s*</sc-view>\s*\*/'
            
            # Search for table comments
            table_matches = re.findall(table_comment_pattern, content, re.IGNORECASE | re.MULTILINE)
            for match in table_matches:
                schema_table = match.upper()
                if '.' in schema_table:
                    schema, table = schema_table.split('.', 1)
                    if table == object_name.upper():
                        found_objects.append((schema_table, 'TABLE', ddl_file))
                        print(f"    ✓ FOUND TABLE: {schema_table} in {ddl_file}")
            
            # Search for view comments
            view_matches = re.findall(view_comment_pattern, content, re.IGNORECASE | re.MULTILINE)
            for match in view_matches:
                schema_view = match.upper()
                if '.' in schema_view:
                    schema, view = schema_view.split('.', 1)
                    if view == object_name.upper():
                        found_objects.append((schema_view, 'VIEW', ddl_file))
                        print(f"    ✓ FOUND VIEW: {schema_view} in {ddl_file}")
            
        except Exception as e:
            print(f"    Error reading {file_path}: {e}")
    
    return found_objects

def extract_ddl_for_object(schema_table, obj_type, ddl_file, ddl_folder):
    """Extract the complete DDL for a specific object"""
    file_path = os.path.join(ddl_folder, ddl_file)
    
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
        
        if obj_type == 'TABLE':
            # Look for table DDL patterns
            patterns = [
                rf'/\*\s*<sc-table>\s+{re.escape(schema_table)}\s*</sc-table>\s*\*/.*?CREATE\s+(?:SET|MULTISET)\s+TABLE\s+{re.escape(schema_table)}.*?;',
            ]
        else:  # VIEW
            # Look for view DDL patterns  
            patterns = [
                rf'/\*\s*<sc-view>\s+{re.escape(schema_table)}\s*</sc-view>\s*\*/.*?REPLACE\s+VIEW\s+{re.escape(schema_table)}.*?;',
            ]
        
        for pattern in patterns:
            matches = re.findall(pattern, content, re.IGNORECASE | re.MULTILINE | re.DOTALL)
            if matches:
                return matches[0].strip()
        
        # Fallback: try to find DDL by searching around the comment
        if obj_type == 'TABLE':
            comment_pattern = rf'/\*\s*<sc-table>\s+{re.escape(schema_table)}\s*</sc-table>\s*\*/'
        else:
            comment_pattern = rf'/\*\s*<sc-view>\s+{re.escape(schema_table)}\s*</sc-view>\s*\*/'
        
        match = re.search(comment_pattern, content, re.IGNORECASE | re.MULTILINE)
        if match:
            start_pos = match.start()
            # Extract until next comment or end of file
            next_comment = re.search(r'/\*\s*<sc-(?:table|view)>', content[match.end():], re.IGNORECASE)
            if next_comment:
                end_pos = match.end() + next_comment.start()
                ddl = content[start_pos:end_pos].strip()
            else:
                # Extract until end or until we find a reasonable stopping point
                ddl_part = content[start_pos:]
                # Try to find the end of the statement
                lines = ddl_part.split('\n')
                ddl_lines = []
                for line in lines:
                    ddl_lines.append(line)
                    if ';' in line and len(ddl_lines) > 10:  # Reasonable stopping point
                        break
                    if line.strip().startswith('--') and line.strip() != '--' and len(ddl_lines) > 10:
                        break
                ddl = '\n'.join(ddl_lines[:500]).strip()  # Limit to reasonable size
            
            return ddl
    
    except Exception as e:
        print(f"    Error extracting DDL from {file_path}: {e}")
    
    return None

def extract_view_dependencies(ddl_content):
    """Extract table dependencies from view DDL"""
    dependencies = set()
    
    # Look for FROM clauses
    from_pattern = r'FROM\s+([A-Z_][A-Z0-9_]*\.[A-Z_][A-Z0-9_]*)'
    matches = re.findall(from_pattern, ddl_content, re.IGNORECASE)
    dependencies.update([m.upper() for m in matches])
    
    # Look for JOIN clauses  
    join_pattern = r'JOIN\s+([A-Z_][A-Z0-9_]*\.[A-Z_][A-Z0-9_]*)'
    matches = re.findall(join_pattern, ddl_content, re.IGNORECASE)
    dependencies.update([m.upper() for m in matches])
    
    return dependencies

def main():
    print("Loading missing objects from JSON file...")
    missing_objects = load_missing_objects()
    print(f"Found {len(missing_objects)} missing objects")
    
    ddl_folder = "../../../CBA_GDW_DDL_Extracts"
    
    found_tables = []
    found_views = []
    view_dependencies = set()
    not_found = []
    
    print(f"\nSearching for DDL in {ddl_folder}...")
    
    for obj in missing_objects:
        if '.' not in obj:
            continue
            
        original_schema, object_name = obj.split('.', 1)
        print(f"\nSearching for {obj} (object name: {object_name})")
        
        # Search by object name only (ignore schema differences)
        found_objects = search_object_in_ddl_files(object_name, ddl_folder)
        
        if found_objects:
            for schema_table, obj_type, ddl_file in found_objects:
                print(f"  Extracting DDL for {schema_table} from {ddl_file}...")
                ddl = extract_ddl_for_object(schema_table, obj_type, ddl_file, ddl_folder)
                
                if ddl:
                    if obj_type == 'TABLE':
                        found_tables.append((f"{obj} -> {schema_table}", ddl))
                        print(f"    ✓ Extracted TABLE DDL for {schema_table}")
                    else:  # VIEW
                        found_views.append((f"{obj} -> {schema_table}", ddl))
                        print(f"    ✓ Extracted VIEW DDL for {schema_table}")
                        # Extract dependencies
                        deps = extract_view_dependencies(ddl)
                        view_dependencies.update(deps)
                else:
                    print(f"    ✗ Could not extract DDL for {schema_table}")
        else:
            print(f"    ✗ NOT FOUND: {obj}")
            not_found.append(obj)
    
    print(f"\n=== SEARCH RESULTS ===")
    print(f"Found tables: {len(found_tables)}")
    print(f"Found views: {len(found_views)}")
    print(f"Not found: {len(not_found)}")
    print(f"View dependencies identified: {len(view_dependencies)}")
    
    # Write table DDLs
    print(f"\nWriting table DDLs to tmp5-tbl.sql...")
    with open('tmp5-tbl.sql', 'w') as f:
        f.write("-- DDL for missing tables and view dependencies\n")
        f.write("-- Generated automatically\n\n")
        
        if found_tables:
            f.write("-- ===============================\n")
            f.write("-- MISSING TABLES\n") 
            f.write("-- ===============================\n\n")
            
            for obj_mapping, ddl in found_tables:
                f.write(f"-- {obj_mapping}\n")
                f.write(ddl)
                f.write("\n\n")
    
    # Write view DDLs
    print(f"Writing view DDLs to tmp5-views.sql...")
    with open('tmp5-views.sql', 'w') as f:
        f.write("-- DDL for missing views\n")
        f.write("-- Generated automatically\n\n")
        
        if found_views:
            f.write("-- ===============================\n")
            f.write("-- MISSING VIEWS\n")
            f.write("-- ===============================\n\n")
            
            for obj_mapping, ddl in found_views:
                f.write(f"-- {obj_mapping}\n")
                f.write(ddl)
                f.write("\n\n")
    
    print(f"\n=== SUMMARY ===")
    print(f"Tables written to tmp5-tbl.sql: {len(found_tables)}")
    print(f"Views written to tmp5-views.sql: {len(found_views)}")
    print(f"Objects not found: {len(not_found)}")
    
    if not_found:
        print(f"\nObjects still not found:")
        for obj in not_found:
            print(f"  - {obj}")

if __name__ == "__main__":
    main() 