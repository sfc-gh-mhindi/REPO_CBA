#!/usr/bin/env python3

import json
import re
import os

def load_json_tables():
    """Load tables from the JSON file"""
    with open('table_extraction_sqlglot.json', 'r') as f:
        data = json.load(f)
    
    all_tables = set()
    results_by_file = data.get('results_by_file', {})
    
    for file_name, tables in results_by_file.items():
        for table in tables:
            all_tables.add(table)
    
    return all_tables, results_by_file

def normalize_schema_name(schema_name):
    """Remove underscores from schema name for table comparison"""
    return schema_name.replace('_', '').upper()

def extract_tables_from_sql_file(file_path):
    """Extract table names from SQL file"""
    tables = set()
    
    try:
        with open(file_path, 'r') as f:
            content = f.read()
            
        # Pattern for CREATE ICEBERG TABLE (handles typos like "reate")
        table_patterns = [
            r'(?:create|reate)\s+iceberg\s+table\s+(?:NPD_D12_DMN_GDWMIG_IBRG\.)?([A-Z_]+)\.([A-Z_][A-Z0-9_]*)',
            r'(?:create|reate)\s+iceberg\s+table\s+(?:PS_CLD_RW\.)?([A-Z_]+)\.([A-Z_][A-Z0-9_]*)',
            r'DROP\s+TABLE\s+IF\s+EXISTS\s+(?:NPD_D12_DMN_GDWMIG_IBRG_V\.)?([A-Z_]+)\.([A-Z_][A-Z0-9_]*)',
            r'DROP\s+TABLE\s+IF\s+EXISTS\s+(?:PS_CLD_RW\.)?([A-Z_]+)\.([A-Z_][A-Z0-9_]*)'
        ]
        
        for pattern in table_patterns:
            matches = re.findall(pattern, content, re.IGNORECASE | re.MULTILINE)
            for match in matches:
                schema, table = match
                tables.add(f"{schema.upper()}.{table.upper()}")
                
    except FileNotFoundError:
        print(f"Warning: File {file_path} not found")
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
    
    return tables

def extract_views_from_sql_file(file_path):
    """Extract view names from SQL file"""
    views = set()
    
    try:
        with open(file_path, 'r') as f:
            content = f.read()
            
        # Pattern for CREATE OR REPLACE VIEW
        view_patterns = [
            r'CREATE\s+(?:OR\s+REPLACE\s+)?VIEW\s+(?:NPD_D12_DMN_GDWMIG_IBRG_V\.)?([A-Z_]+)\.([A-Z_][A-Z0-9_]*)',
            r'CREATE\s+(?:OR\s+REPLACE\s+)?VIEW\s+(?:ps_gdw1_bteq\.)?([A-Z_]+)\.([A-Z_][A-Z0-9_]*)',
            r'CREATE\s+(?:OR\s+REPLACE\s+)?VIEW\s+([A-Z_]+)\.([A-Z_][A-Z0-9_]*)'
        ]
        
        for pattern in view_patterns:
            matches = re.findall(pattern, content, re.IGNORECASE | re.MULTILINE)
            for match in matches:
                schema, view = match
                views.add(f"{schema.upper()}.{view.upper()}")
                
    except FileNotFoundError:
        print(f"Warning: File {file_path} not found")
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
    
    return views

def main():
    print("Loading tables from JSON file...")
    all_json_tables, results_by_file = load_json_tables()
    print(f"Found {len(all_json_tables)} unique tables in JSON file")
    
    print("\nExtracting tables from SQL files...")
    # Extract from table SQL files
    table_sql_files = [
        "../../../Migrating 140 Tables/After CDL Issue was Reverted/CBA01-Creating Iceberg Tables.sql",
        "../CBA01.1 - Missing Tables.sql"
    ]
    
    all_sql_tables = set()
    for sql_file in table_sql_files:
        print(f"Processing table file: {sql_file}")
        tables = extract_tables_from_sql_file(sql_file)
        print(f"  Found {len(tables)} tables")
        all_sql_tables.update(tables)
    
    print(f"Total tables found in table SQL files: {len(all_sql_tables)}")
    
    print("\nExtracting views from SQL files...")
    # Extract from view SQL files  
    view_sql_files = [
        "../../../Migrating 140 Tables/After CDL Issue was Reverted/CBA02-Creating Views.sql",
        "../CBA02.1 - Missing Views.sql"
    ]
    
    all_sql_views = set()
    for sql_file in view_sql_files:
        print(f"Processing view file: {sql_file}")
        views = extract_views_from_sql_file(sql_file)
        print(f"  Found {len(views)} views")
        all_sql_views.update(views)
    
    print(f"Total views found in view SQL files: {len(all_sql_views)}")
    
    print("\nComparing JSON tables against SQL tables and views...")
    
    # Find matches and missing tables
    found_as_tables = set()
    found_as_views = set()
    missing_tables = set()
    
    for json_table in all_json_tables:
        if '.' not in json_table:
            continue
            
        db_name, table_name = json_table.split('.', 1)
        
        # Check if exists as table (with underscore normalization)
        normalized_db = normalize_schema_name(db_name)
        normalized_table_ref = f"{normalized_db}.{table_name.upper()}"
        
        table_found = False
        for sql_table in all_sql_tables:
            if sql_table.upper() == normalized_table_ref:
                found_as_tables.add(json_table)
                table_found = True
                print(f"FOUND AS TABLE: {json_table} -> {sql_table}")
                break
        
        # Check if exists as view (without underscore normalization)
        if not table_found:
            view_ref = f"{db_name.upper()}.{table_name.upper()}"
            view_found = False
            for sql_view in all_sql_views:
                if sql_view.upper() == view_ref:
                    found_as_views.add(json_table)
                    view_found = True
                    print(f"FOUND AS VIEW: {json_table} -> {sql_view}")
                    break
            
            if not view_found:
                missing_tables.add(json_table)
    
    print(f"\n=== SUMMARY ===")
    print(f"Total tables in JSON: {len(all_json_tables)}")
    print(f"Found as tables: {len(found_as_tables)}")
    print(f"Found as views: {len(found_as_views)}")
    print(f"Missing (not found as either table or view): {len(missing_tables)}")
    
    # Create the final output
    final_missing = sorted(list(missing_tables))
    
    output_data = {
        "description": "Tables referenced in BTEQ files but not found in any SQL files (neither as tables nor views)",
        "total_tables_in_json": len(all_json_tables),
        "found_as_tables": len(found_as_tables), 
        "found_as_views": len(found_as_views),
        "missing_count": len(final_missing),
        "missing_tables": final_missing,
        "summary": {
            "table_sql_files_checked": table_sql_files,
            "view_sql_files_checked": view_sql_files,
            "total_tables_found_in_sql": len(all_sql_tables),
            "total_views_found_in_sql": len(all_sql_views)
        }
    }
    
    # Write to JSON file
    with open('referenced_forth_wave_updated.json', 'w') as f:
        json.dump(output_data, f, indent=2)
    
    print(f"\nResults written to: referenced_forth_wave_updated.json")
    print(f"Missing tables: {len(final_missing)}")

if __name__ == "__main__":
    main() 