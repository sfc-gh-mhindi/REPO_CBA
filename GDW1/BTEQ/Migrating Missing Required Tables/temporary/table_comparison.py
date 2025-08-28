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
    """Remove underscores from schema name for comparison"""
    return schema_name.replace('_', '')

def extract_tables_from_sql_file(file_path):
    """Extract table names from SQL file"""
    tables = set()
    
    try:
        with open(file_path, 'r') as f:
            content = f.read()
        
        # Pattern to match CREATE ICEBERG TABLE statements
        # Handles both NPD_D12_DMN_GDWMIG_IBRG.SCHEMA.TABLE and PS_CLD_RW.SCHEMA.TABLE patterns
        pattern = r'CREATE\s+ICEBERG\s+TABLE\s+(?:[^.]+\.)?([^.]+)\.([^\s(]+)'
        matches = re.findall(pattern, content, re.IGNORECASE)
        
        for schema, table in matches:
            # Normalize schema name by removing underscores
            normalized_schema = normalize_schema_name(schema.upper())
            table_name = table.upper()
            tables.add(f"{normalized_schema}.{table_name}")
        
    except FileNotFoundError:
        print(f"File not found: {file_path}")
    
    return tables

def main():
    # Load tables from JSON
    json_tables, results_by_file = load_json_tables()
    
    # Load tables from both SQL files - fix the paths
    cba01_creating_tables = extract_tables_from_sql_file('../../Migrating 140 Tables/After CDL Issue was Reverted/CBA01-Creating Iceberg Tables.sql')
    cba01_missing_tables = extract_tables_from_sql_file('../CBA01.1 - Missing Tables.sql')
    
    # Combine both SQL files
    sql_tables = cba01_creating_tables.union(cba01_missing_tables)
    
    # Normalize JSON tables for comparison
    normalized_json_tables = set()
    original_to_normalized = {}
    
    for table in json_tables:
        if '.' in table:
            schema, table_name = table.split('.', 1)
            normalized_schema = normalize_schema_name(schema.upper())
            normalized_table = f"{normalized_schema}.{table_name.upper()}"
            normalized_json_tables.add(normalized_table)
            original_to_normalized[table] = normalized_table
    
    # Find tables in JSON but not in SQL files
    missing_tables = normalized_json_tables - sql_tables
    
    # Create reverse mapping for reporting
    normalized_to_original = {v: k for k, v in original_to_normalized.items()}
    
    print("=== COMPARISON RESULTS ===")
    print(f"Total unique tables in JSON: {len(normalized_json_tables)}")
    print(f"Total tables in CBA01-Creating Iceberg Tables.sql: {len(cba01_creating_tables)}")
    print(f"Total tables in CBA01.1 - Missing Tables.sql: {len(cba01_missing_tables)}")
    print(f"Combined SQL tables: {len(sql_tables)}")
    print(f"Missing tables (in JSON but not in SQL files): {len(missing_tables)}")
    
    if missing_tables:
        print("\n=== MISSING TABLES ===")
        for table in sorted(missing_tables):
            original_name = normalized_to_original.get(table, table)
            print(f"- {original_name}")
        
        # Group by BTEQ file for better understanding - only show summary
        print("\n=== SUMMARY BY SCHEMA ===")
        missing_by_schema = {}
        for table in missing_tables:
            schema = table.split('.')[0]
            if schema not in missing_by_schema:
                missing_by_schema[schema] = 0
            missing_by_schema[schema] += 1
        
        for schema, count in sorted(missing_by_schema.items()):
            print(f"{schema}: {count} tables")
    
    else:
        print("\nAll tables from JSON are present in the SQL files!")
    
    # Also show some examples of found tables for verification
    print("\n=== SAMPLE OF MATCHED TABLES ===")
    matched_tables = normalized_json_tables.intersection(sql_tables)
    for table in sorted(list(matched_tables)[:10]):  # Show first 10
        original_name = normalized_to_original.get(table, table)
        print(f"✓ {original_name}")

if __name__ == "__main__":
    main() 