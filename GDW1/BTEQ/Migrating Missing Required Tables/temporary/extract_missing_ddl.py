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

def search_ddl_in_file(file_path, schema_name, object_name):
    """Search for DDL of a specific object in a DDL file"""
    print(f"    Searching in {os.path.basename(file_path)} for {schema_name}.{object_name}")
    
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
        
        # Patterns to match table/view creation
        patterns = [
            # Standard CREATE TABLE pattern
            rf'CREATE\s+(?:MULTISET\s+)?TABLE\s+{re.escape(schema_name)}\.{re.escape(object_name)}\s*\(',
            rf'CREATE\s+(?:MULTISET\s+)?TABLE\s+{re.escape(schema_name)}\.{re.escape(object_name)}\s*,',
            rf'CREATE\s+(?:MULTISET\s+)?TABLE\s+{re.escape(schema_name)}\.{re.escape(object_name)}\s+\(',
            # CREATE VIEW pattern
            rf'CREATE\s+(?:OR\s+REPLACE\s+)?VIEW\s+{re.escape(schema_name)}\.{re.escape(object_name)}\s*\(',
            rf'CREATE\s+(?:OR\s+REPLACE\s+)?VIEW\s+{re.escape(schema_name)}\.{re.escape(object_name)}\s*,',
            rf'CREATE\s+(?:OR\s+REPLACE\s+)?VIEW\s+{re.escape(schema_name)}\.{re.escape(object_name)}\s+\(',
            # Alternative patterns without schema
            rf'CREATE\s+(?:MULTISET\s+)?TABLE\s+{re.escape(object_name)}\s*\(',
            rf'CREATE\s+(?:OR\s+REPLACE\s+)?VIEW\s+{re.escape(object_name)}\s*\(',
        ]
        
        for pattern in patterns:
            matches = list(re.finditer(pattern, content, re.IGNORECASE | re.MULTILINE))
            if matches:
                # Found the object, now extract the full DDL
                match = matches[0]
                start_pos = match.start()
                
                # Find the complete DDL statement
                ddl = extract_complete_ddl(content, start_pos)
                if ddl:
                    print(f"    ✓ FOUND {schema_name}.{object_name}")
                    return ddl.strip()
        
        return None
        
    except Exception as e:
        print(f"    Error reading {file_path}: {e}")
        return None

def extract_complete_ddl(content, start_pos):
    """Extract the complete DDL statement starting from start_pos"""
    # Find the beginning of the statement
    while start_pos > 0 and content[start_pos-1] not in '\n\r':
        start_pos -= 1
    
    # Extract until we find the end (semicolon followed by newline or EOF)
    paren_count = 0
    in_string = False
    quote_char = None
    i = start_pos
    
    while i < len(content):
        char = content[i]
        
        if not in_string:
            if char in ("'", '"'):
                in_string = True
                quote_char = char
            elif char == '(':
                paren_count += 1
            elif char == ')':
                paren_count -= 1
            elif char == ';' and paren_count <= 0:
                # Found end of statement
                return content[start_pos:i+1]
        else:
            if char == quote_char and (i == 0 or content[i-1] != '\\'):
                in_string = False
                quote_char = None
        
        i += 1
    
    # If we reach here, return the rest of the content
    return content[start_pos:]

def find_ddl_for_object(schema_name, object_name, ddl_folder):
    """Find DDL for a specific object in all DDL files"""
    
    # Define search order - check tables first, then views
    ddl_files = [
        'DDL_Tables.sql',
        'DDL_Tables1.sql', 
        'DDL_Tables2.sql',
        'DDL_Views.sql',
        'DDL_Views1.sql',
        'DDL_Views2.sql'
    ]
    
    for ddl_file in ddl_files:
        file_path = os.path.join(ddl_folder, ddl_file)
        if os.path.exists(file_path):
            ddl = search_ddl_in_file(file_path, schema_name, object_name)
            if ddl:
                return ddl, 'TABLE' if 'Table' in ddl_file else 'VIEW'
    
    return None, None

def extract_view_dependencies(ddl_content):
    """Extract table dependencies from view DDL"""
    dependencies = set()
    
    # Look for FROM clauses
    from_pattern = r'FROM\s+([A-Z_][A-Z0-9_]*(?:\.[A-Z_][A-Z0-9_]*)?)'
    matches = re.findall(from_pattern, ddl_content, re.IGNORECASE)
    
    for match in matches:
        if '.' in match:
            dependencies.add(match.upper())
        else:
            # Add common schema prefixes if no schema specified
            for schema in ['PVTECH', 'PDDSTG', 'STAR_CAD_PROD_DATA', 'DGRDDB']:
                dependencies.add(f"{schema}.{match.upper()}")
    
    # Look for JOIN clauses  
    join_pattern = r'JOIN\s+([A-Z_][A-Z0-9_]*(?:\.[A-Z_][A-Z0-9_]*)?)'
    matches = re.findall(join_pattern, ddl_content, re.IGNORECASE)
    
    for match in matches:
        if '.' in match:
            dependencies.add(match.upper())
        else:
            for schema in ['PVTECH', 'PDDSTG', 'STAR_CAD_PROD_DATA', 'DGRDDB']:
                dependencies.add(f"{schema}.{match.upper()}")
    
    return dependencies

def main():
    print("Loading missing objects from JSON file...")
    missing_objects = load_missing_objects()
    print(f"Found {len(missing_objects)} missing objects")
    
    ddl_folder = "../../../../CBA_GDW_DDL_Extracts"
    
    found_tables = []
    found_views = []
    view_dependencies = set()
    not_found = []
    
    print(f"\nSearching for DDL in {ddl_folder}...")
    
    for obj in missing_objects:
        if '.' not in obj:
            continue
            
        schema_name, object_name = obj.split('.', 1)
        print(f"\nSearching for {schema_name}.{object_name}")
        
        ddl, obj_type = find_ddl_for_object(schema_name, object_name, ddl_folder)
        
        if ddl:
            if obj_type == 'TABLE':
                found_tables.append((obj, ddl))
            else:  # VIEW
                found_views.append((obj, ddl))
                # Extract dependencies from view
                deps = extract_view_dependencies(ddl)
                view_dependencies.update(deps)
        else:
            print(f"    ✗ NOT FOUND: {schema_name}.{object_name}")
            not_found.append(obj)
    
    print(f"\n=== SEARCH RESULTS ===")
    print(f"Found tables: {len(found_tables)}")
    print(f"Found views: {len(found_views)}")
    print(f"Not found: {len(not_found)}")
    print(f"View dependencies identified: {len(view_dependencies)}")
    
    # Now search for view dependencies
    print(f"\nSearching for view dependencies...")
    dependency_ddls = []
    
    for dep in view_dependencies:
        if '.' in dep:
            schema_name, object_name = dep.split('.', 1)
            print(f"  Searching for dependency: {schema_name}.{object_name}")
            
            ddl, obj_type = find_ddl_for_object(schema_name, object_name, ddl_folder)
            if ddl and obj_type == 'TABLE':
                dependency_ddls.append((dep, ddl))
                print(f"    ✓ Found dependency: {dep}")
    
    # Write table DDLs
    print(f"\nWriting table DDLs to tmp5-tbl.sql...")
    with open('tmp5-tbl.sql', 'w') as f:
        f.write("-- DDL for missing tables and view dependencies\n")
        f.write("-- Generated automatically\n\n")
        
        # Write main tables
        if found_tables:
            f.write("-- ===============================\n")
            f.write("-- MISSING TABLES\n") 
            f.write("-- ===============================\n\n")
            
            for obj, ddl in found_tables:
                f.write(f"-- {obj}\n")
                f.write(ddl)
                f.write("\n\n")
        
        # Write dependency tables
        if dependency_ddls:
            f.write("-- ===============================\n")
            f.write("-- VIEW DEPENDENCIES (TABLES)\n")
            f.write("-- ===============================\n\n")
            
            for obj, ddl in dependency_ddls:
                f.write(f"-- {obj}\n")
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
            
            for obj, ddl in found_views:
                f.write(f"-- {obj}\n")
                f.write(ddl)
                f.write("\n\n")
    
    print(f"\n=== SUMMARY ===")
    print(f"Tables written to tmp5-tbl.sql: {len(found_tables) + len(dependency_ddls)}")
    print(f"Views written to tmp5-views.sql: {len(found_views)}")
    print(f"Objects not found: {len(not_found)}")
    
    if not_found:
        print(f"\nObjects not found:")
        for obj in not_found:
            print(f"  - {obj}")

if __name__ == "__main__":
    main() 