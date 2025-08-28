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
        patterns = [
            r'CREATE\s+ICEBERG\s+TABLE\s+(?:NPD_D12_DMN_GDWMIG_IBRG_V\.)?(\w+)\.(\w+)',
            r'DROP\s+TABLE\s+IF\s+EXISTS\s+(?:NPD_D12_DMN_GDWMIG_IBRG_V\.)?(\w+)\.(\w+)',
            r'CREATE\s+TABLE\s+(?:NPD_D12_DMN_GDWMIG_IBRG_V\.)?(\w+)\.(\w+)'
        ]
        
        for pattern in patterns:
            matches = re.finditer(pattern, content, re.IGNORECASE)
            for match in matches:
                schema = normalize_schema_name(match.group(1))
                table = match.group(2)
                tables.add(f"{schema}.{table}")
        
    except FileNotFoundError:
        print(f"Warning: File {file_path} not found")
    
    return tables

def main():
    print("Loading tables from JSON file...")
    json_tables, results_by_file = load_json_tables()
    
    print("Extracting tables from SQL files...")
    # Extract tables from both SQL files
    sql_file1 = "../CBA01.1 - Missing Tables.sql"
    sql_file2 = "../../Migrating 140 Tables/After CDL Issue was Reverted/CBA01-Creating Iceberg Tables.sql"
    
    sql_tables1 = extract_tables_from_sql_file(sql_file1)
    sql_tables2 = extract_tables_from_sql_file(sql_file2)
    
    print(f"Tables found in CBA01.1 - Missing Tables.sql: {len(sql_tables1)}")
    print(f"Tables found in CBA01-Creating Iceberg Tables.sql: {len(sql_tables2)}")
    
    # Combine all SQL tables
    all_sql_tables = sql_tables1.union(sql_tables2)
    print(f"Total unique tables in SQL files: {len(all_sql_tables)}")
    
    # Find missing tables
    missing_tables = []
    
    for json_table in json_tables:
        # Extract db_name and table_name from JSON format (db_name.table_name)
        parts = json_table.split('.')
        if len(parts) == 2:
            db_name, table_name = parts
            # Normalize for comparison (remove underscores)
            normalized_key = f"{normalize_schema_name(db_name)}.{table_name}"
            
            if normalized_key not in all_sql_tables:
                missing_tables.append(json_table)  # Keep original format
    
    # Sort the missing tables
    missing_tables.sort()
    
    # Create the output JSON structure
    output_data = {
        "description": "Tables referenced in BTEQ files but not found in either CBA01.1 - Missing Tables.sql or CBA01-Creating Iceberg Tables.sql",
        "total_missing_tables": len(missing_tables),
        "missing_tables": missing_tables
    }
    
    # Write to JSON file
    with open('referenced_forth_wave.json', 'w') as f:
        json.dump(output_data, f, indent=2)
    
    print(f"\n✅ Created referenced_forth_wave.json with {len(missing_tables)} missing tables")
    print("Sample missing tables:")
    for i, table in enumerate(missing_tables[:10]):
        print(f"  {table}")
    if len(missing_tables) > 10:
        print(f"  ... and {len(missing_tables) - 10} more")

if __name__ == "__main__":
    main() 