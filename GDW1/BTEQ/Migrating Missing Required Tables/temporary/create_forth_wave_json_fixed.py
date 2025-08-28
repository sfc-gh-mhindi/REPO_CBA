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
    return schema_name.replace('_', '').upper()

def extract_tables_from_sql_file(file_path):
    """Extract table names from SQL file"""
    tables = set()
    
    try:
        with open(file_path, 'r') as f:
            content = f.read()
            
        # Pattern to match CREATE ICEBERG TABLE statements
        # Look for patterns like: CREATE ICEBERG TABLE database.schema.table_name
        pattern = r'(?i)create\s+(?:or\s+replace\s+)?iceberg\s+table\s+(?:[a-z0-9_]+\.)?([a-z0-9_]+)\.([a-z0-9_]+)'
        
        matches = re.findall(pattern, content)
        for schema, table in matches:
            # Normalize schema name by removing underscores and converting to uppercase
            normalized_schema = normalize_schema_name(schema)
            tables.add(f"{normalized_schema}.{table.upper()}")
            
    except FileNotFoundError:
        print(f"Warning: File not found: {file_path}")
        return set()
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return set()
    
    return tables

def parse_json_table(table_name):
    """Parse table name from JSON format (db_name.table_name)"""
    if '.' not in table_name:
        return None, table_name.upper()
    
    parts = table_name.split('.')
    if len(parts) != 2:
        return None, table_name.upper()
    
    db_name, table = parts
    # Normalize db_name by removing underscores and converting to uppercase
    normalized_db = normalize_schema_name(db_name)
    return normalized_db, table.upper()

def main():
    print("Loading tables from JSON file...")
    json_tables, results_by_file = load_json_tables()
    
    print(f"Found {len(json_tables)} unique tables in JSON file")
    
    # Extract tables from both SQL files
    print("Extracting tables from SQL files...")
    sql_file_paths = [
        '../CBA01.1 - Missing Tables.sql',
        '../../../Migrating 140 Tables/After CDL Issue was Reverted/CBA01-Creating Iceberg Tables.sql'
    ]
    
    existing_tables = set()
    for sql_file in sql_file_paths:
        tables_in_file = extract_tables_from_sql_file(sql_file)
        existing_tables.update(tables_in_file)
        print(f"Found {len(tables_in_file)} tables in {sql_file}")
    
    print(f"Total existing tables in SQL files: {len(existing_tables)}")
    
    # Find missing tables
    missing_tables = []
    
    for json_table in json_tables:
        normalized_db, table_name = parse_json_table(json_table)
        if normalized_db is None:
            print(f"Warning: Could not parse table name: {json_table}")
            missing_tables.append(json_table)
            continue
        
        # Create the normalized format for comparison
        normalized_table = f"{normalized_db}.{table_name}"
        
        if normalized_table not in existing_tables:
            missing_tables.append(json_table)
        else:
            print(f"Found match: {json_table} -> {normalized_table}")
    
    # Sort the missing tables
    missing_tables.sort()
    
    # Create output JSON
    output_data = {
        "description": "Tables referenced in BTEQ files but missing from both CBA01.1 - Missing Tables.sql and CBA01-Creating Iceberg Tables.sql",
        "total_missing_tables": len(missing_tables),
        "missing_tables": missing_tables
    }
    
    # Write to file
    with open('referenced_forth_wave_fixed.json', 'w') as f:
        json.dump(output_data, f, indent=2)
    
    print(f"\n=== SUMMARY ===")
    print(f"Total tables in JSON file: {len(json_tables)}")
    print(f"Total existing tables in SQL files: {len(existing_tables)}")
    print(f"Missing tables: {len(missing_tables)}")
    print(f"Results written to referenced_forth_wave_fixed.json")
    
    # Print some examples of missing tables
    if missing_tables:
        print(f"\nFirst 10 missing tables:")
        for table in missing_tables[:10]:
            print(f"  - {table}")

if __name__ == "__main__":
    main() 