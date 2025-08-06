#!/usr/bin/env python3
"""
Script to create backup standard tables using CREATE TABLE AS SELECT
to copy data from iceberg tables in 01-Creating Iceberg Tables.sql
"""

import re
import os

def extract_iceberg_tables(iceberg_sql_path):
    """Extract iceberg table names from the SQL file"""
    with open(iceberg_sql_path, 'r') as f:
        content = f.read()
    
    # Pattern to match iceberg table creation statements with quoted names
    pattern = r'create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"\.("[\w_]+")\.("[\w_]+")\s*\('
    matches = re.findall(pattern, content, re.IGNORECASE)
    
    backup_tables = []
    seen_tables = set()  # To avoid duplicates
    
    for schema_quoted, table_quoted in matches:
        # Remove quotes from schema and table names for cleaner backup table names
        schema_clean = schema_quoted.strip('"')  # e.g., pdcbods
        table_clean = table_quoted.strip('"')    # e.g., ods_rule
        
        # Create unique identifier to avoid duplicates
        table_id = f"{schema_clean}_{table_clean}"
        
        if table_id in seen_tables:
            continue  # Skip duplicates
        seen_tables.add(table_id)
        
        # Create the backup table definition using CREATE TABLE AS SELECT
        backup_table_sql = f"""-- Backup data from {schema_quoted}.{table_quoted}
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.{table_id} AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG".{schema_quoted}.{table_quoted};
"""
        backup_tables.append(backup_table_sql)
    
    return backup_tables

def create_backup_sql_file(backup_tables, output_path):
    """Create the backup SQL file"""
    
    header = """-- =====================================================================
-- BACKUP ICEBERG TABLES DATA SCRIPT
-- =====================================================================
-- This script creates standard backup tables with data copied from 
-- iceberg tables using CREATE TABLE AS SELECT statements
-- 
-- Target schema: NPD_D12_DMN_GDWMIG.TMP
-- Table naming convention: {schema_name}_{table_name}
-- Example: "pdcbods"."ods_rule" becomes pdcbods_ods_rule
-- =====================================================================

USE ROLE r_dev_npd_d12_gdwmig;
USE DATABASE NPD_D12_DMN_GDWMIG;
USE WAREHOUSE wh_usr_npd_d12_gdwmig_001;

-- Create TMP schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS TMP;

"""
    
    with open(output_path, 'w') as f:
        f.write(header)
        f.write('\n'.join(backup_tables))
    
    print(f"Backup CTAS script created: {output_path}")
    print(f"Number of backup tables: {len(backup_tables)}")

def main():
    # File paths
    iceberg_sql = "../01-Creating Iceberg Tables.sql"
    backup_sql = "05-Creating-Backup-Tables-CTAS.sql"
    
    print("Extracting iceberg table names...")
    backup_tables = extract_iceberg_tables(iceberg_sql)
    
    print("Creating backup CTAS SQL file...")
    create_backup_sql_file(backup_tables, backup_sql)
    
    print("Done!")

if __name__ == "__main__":
    main() 