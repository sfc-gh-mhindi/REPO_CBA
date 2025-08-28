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
        
        # More flexible regex to handle typos like "reate" instead of "create"
        # Also handle both "create iceberg table" and "CREATE ICEBERG TABLE"
        patterns = [
            r'(?:create|reate)\s+iceberg\s+table\s+(NPD_D12_DMN_GDWMIG_IBRG\.[\w]+\.[\w]+)',
            r'(?:CREATE|REATE)\s+ICEBERG\s+TABLE\s+(NPD_D12_DMN_GDWMIG_IBRG\.[\w]+\.[\w]+)',
            r'(?:create|reate)\s+iceberg\s+table\s+(PS_CLD_RW\.[\w]+\.[\w]+)',
            r'(?:CREATE|REATE)\s+ICEBERG\s+TABLE\s+(PS_CLD_RW\.[\w]+\.[\w]+)',
        ]
        
        for pattern in patterns:
            matches = re.finditer(pattern, content, re.IGNORECASE)
            for match in matches:
                full_table_name = match.group(1)
                
                # Extract schema.table_name from patterns like NPD_D12_DMN_GDWMIG_IBRG.SCHEMA.TABLE or PS_CLD_RW.SCHEMA.TABLE
                schema_table_match = re.search(r'(?:NPD_D12_DMN_GDWMIG_IBRG|PS_CLD_RW)\.([^\.]+)\.([^\.]+)', full_table_name)
                if schema_table_match:
                    schema = schema_table_match.group(1).strip()
                    table = schema_table_match.group(2).strip()
                    # Normalize schema by removing underscores for comparison
                    normalized_schema = normalize_schema_name(schema)
                    tables.add(f"{normalized_schema}.{table}")
        
        print(f"Extracted {len(tables)} tables from {file_path}")
        return tables
        
    except FileNotFoundError:
        print(f"File not found: {file_path}")
        return set()
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return set()

def main():
    print("Loading tables from JSON file...")
    json_tables, results_by_file = load_json_tables()
    print(f"Total unique tables in JSON: {len(json_tables)}")
    
    print("\nExtracting tables from SQL files...")
    
    # Extract from both SQL files
    sql_file1 = "../../../Migrating 140 Tables/After CDL Issue was Reverted/CBA01-Creating Iceberg Tables.sql"
    sql_file2 = "../CBA01.1 - Missing Tables.sql"
    
    found_tables = set()
    found_tables.update(extract_tables_from_sql_file(sql_file1))
    found_tables.update(extract_tables_from_sql_file(sql_file2))
    
    print(f"Total tables found in SQL files: {len(found_tables)}")
    
    # Find missing tables by normalizing database names from JSON
    missing_tables = []
    
    print("\nComparing tables...")
    
    for table in json_tables:
        if '.' in table:
            # Split database.table_name
            db_name, table_name = table.split('.', 1)
            # Normalize database name by removing underscores
            normalized_db_name = normalize_schema_name(db_name)
            normalized_table = f"{normalized_db_name}.{table_name}"
        else:
            # Handle unqualified tables
            normalized_table = table
        
        # Check if normalized table exists in found tables
        if normalized_table not in found_tables:
            missing_tables.append(table)  # Keep original format for output
        else:
            print(f"FOUND: {table} -> {normalized_table}")
    
    # Sort the missing tables
    missing_tables.sort()
    
    print(f"\nMissing tables: {len(missing_tables)}")
    for table in missing_tables[:10]:  # Show first 10 as sample
        if '.' in table:
            db_name, table_name = table.split('.', 1)
            normalized_db_name = normalize_schema_name(db_name)
            print(f"  {table} -> would normalize to {normalized_db_name}.{table_name}")
        else:
            print(f"  {table}")
    
    if len(missing_tables) > 10:
        print(f"  ... and {len(missing_tables) - 10} more")
    
    # Create the output JSON
    output_data = {
        "description": "Tables referenced in BTEQ files but not found in CBA01-Creating Iceberg Tables.sql or CBA01.1 - Missing Tables.sql",
        "note": "Database names are normalized by removing underscores for comparison (e.g., STAR_CAD_PROD_DATA -> STARCADPRODDATA)",
        "comparison_details": {
            "total_tables_in_json": len(json_tables),
            "total_tables_found_in_sql": len(found_tables),
            "missing_count": len(missing_tables)
        },
        "missing_tables": missing_tables
    }
    
    # Write to JSON file
    output_file = 'referenced_forth_wave_final.json'
    with open(output_file, 'w') as f:
        json.dump(output_data, f, indent=2)
    
    print(f"\nResults written to {output_file}")
    
    # Show some examples of found tables for verification
    print(f"\nSample of FOUND tables (showing database name normalization):")
    found_list = sorted(list(found_tables))
    for table in found_list[:5]:
        print(f"  {table}")

if __name__ == "__main__":
    main() 