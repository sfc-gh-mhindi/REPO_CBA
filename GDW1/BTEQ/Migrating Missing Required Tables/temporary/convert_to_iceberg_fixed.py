#!/usr/bin/env python3

import re
import os

def convert_teradata_type_to_snowflake(td_type):
    """Convert Teradata data type to Snowflake equivalent"""
    td_type = td_type.strip()
    
    # Handle VARCHAR and CHAR -> STRING
    if 'VARCHAR' in td_type.upper():
        return 'STRING'
    elif 'CHAR(' in td_type.upper():
        return 'STRING'
    elif td_type.upper().startswith('CHAR'):
        return 'STRING'
    
    # Handle DECIMAL with precision
    decimal_match = re.match(r'DECIMAL\((\d+),(\d+)\)', td_type.upper())
    if decimal_match:
        precision, scale = decimal_match.groups()
        return f'DECIMAL({precision},{scale})'
    
    # Handle other numeric types
    if td_type.upper() == 'INTEGER':
        return 'INTEGER'
    elif td_type.upper() == 'SMALLINT':
        return 'INT'  # Convert SMALLINT to INT
    elif td_type.upper() == 'BIGINT':
        return 'BIGINT'
    elif 'DECIMAL(' in td_type.upper():
        match = re.search(r'DECIMAL\((\d+),(\d+)\)', td_type.upper())
        if match:
            precision, scale = match.groups()
            return f'DECIMAL({precision},{scale})'
        else:
            return 'DECIMAL(38,2)'  # Default precision
    
    # Handle DATE and TIMESTAMP
    if td_type.upper() == 'DATE':
        return 'DATE'
    elif 'TIMESTAMP' in td_type.upper():
        if '(6)' in td_type:
            return 'TIMESTAMP(6)'
        else:
            return 'TIMESTAMP_NTZ'
    
    # Default fallback
    return 'STRING'

def extract_table_info_from_comment(comment_line):
    """Extract schema and table name from comment"""
    match = re.search(r'<sc-table>\s+([A-Z_][A-Z0-9_]*\.[A-Z_][A-Z0-9_]*)\s*</sc-table>', comment_line)
    if match:
        schema_table = match.group(1)
        if '.' in schema_table:
            schema, table = schema_table.split('.', 1)
            return schema.upper(), table.upper()
    return None, None

def parse_teradata_column_definition(column_line):
    """Parse a Teradata column definition line"""
    column_line = column_line.strip()
    if not column_line or column_line.startswith('PRIMARY INDEX') or column_line.startswith('INDEX') or column_line.startswith(')'):
        return None
    
    # Remove trailing comma
    if column_line.endswith(','):
        column_line = column_line[:-1]
    
    # Skip empty lines or lines with just parentheses
    if not column_line or column_line == '(' or column_line == ')':
        return None
    
    # Extract column name (first word)
    parts = column_line.split()
    if len(parts) < 2:
        return None
        
    column_name = parts[0].strip()
    
    # Find the data type - look for common Teradata patterns
    remainder = ' '.join(parts[1:])
    
    # Handle various data type patterns
    type_patterns = [
        r'^(VARCHAR\(\d+\))',
        r'^(CHAR\(\d+\))', 
        r'^(DECIMAL\(\d+,\d+\))',
        r'^(DECIMAL\(\d+\))',
        r'^(TIMESTAMP\(\d+\))',
        r'^(INTEGER|SMALLINT|BIGINT|DATE|TIMESTAMP)',
        r'^(CHAR|VARCHAR)'
    ]
    
    td_type = None
    for pattern in type_patterns:
        match = re.match(pattern, remainder.upper())
        if match:
            td_type = match.group(1)
            break
    
    if not td_type:
        # Fallback: assume first word after column name is the type
        td_type = parts[1].strip() if len(parts) > 1 else 'STRING'
    
    sf_type = convert_teradata_type_to_snowflake(td_type)
    
    # Check for NOT NULL
    not_null = 'NOT NULL' if 'NOT NULL' in remainder.upper() else ''
    
    # Extract TITLE/COMMENT if present
    title_match = re.search(r"TITLE\s+'([^']*)'", remainder)
    comment = f" COMMENT '{title_match.group(1)}'" if title_match else ''
    
    return {
        'name': column_name,
        'type': sf_type,
        'not_null': not_null,
        'comment': comment
    }

def convert_teradata_table_to_iceberg(table_ddl):
    """Convert a single Teradata table DDL to Snowflake Iceberg format"""
    lines = table_ddl.strip().split('\n')
    
    # Find the comment line with schema.table info
    schema_name = None
    table_name = None
    original_mapping = None
    
    for line in lines:
        if '<sc-table>' in line:
            schema_name, table_name = extract_table_info_from_comment(line)
            break
        elif line.startswith('-- ') and ' -> ' in line:
            # Extract mapping info
            original_mapping = line[3:].strip()
            if ' -> ' in original_mapping:
                orig_table, actual_table = original_mapping.split(' -> ', 1)
                if '.' in actual_table:
                    schema_name, table_name = actual_table.split('.', 1)
                    schema_name = schema_name.upper()
                    table_name = table_name.upper()
    
    if not schema_name or not table_name:
        return None
        
    # Parse column definitions - look for the CREATE TABLE block
    columns = []
    in_column_section = False
    create_table_found = False
    
    for i, line in enumerate(lines):
        line = line.strip()
        
        # Detect start of CREATE TABLE
        if 'CREATE' in line.upper() and 'TABLE' in line.upper() and '(' in line:
            in_column_section = True
            create_table_found = True
            continue
        elif 'CREATE' in line.upper() and 'TABLE' in line.upper():
            create_table_found = True
            # Look for opening parenthesis in next few lines
            for j in range(i+1, min(i+5, len(lines))):
                if '(' in lines[j]:
                    in_column_section = True
                    break
            continue
        elif create_table_found and '(' in line and not in_column_section:
            in_column_section = True
            continue
        elif line.startswith('PRIMARY INDEX') or line.startswith('INDEX') or line.startswith(')'):
            if in_column_section:
                break
        elif in_column_section and line:
            column_info = parse_teradata_column_definition(line)
            if column_info:
                columns.append(column_info)
    
    if not columns:
        # Debug: print the DDL that couldn't be parsed
        print(f"Warning: No columns found for {schema_name}.{table_name}")
        return None
        
    # Generate Snowflake DDL
    sf_ddl = []
    
    # Add comment about original mapping
    if original_mapping:
        sf_ddl.append(f"/* {original_mapping} */")
    
    sf_ddl.append("--** SSC-FDM-TD0024 - SET TABLE FUNCTIONALITY NOT SUPPORTED. TABLE MIGHT HAVE DUPLICATE ROWS **")
    sf_ddl.append("--** CONVERTED TO ICEBERG TABLE FORMAT **")
    sf_ddl.append(f"CREATE ICEBERG TABLE PS_CLD_RW.{schema_name}.{table_name} (")
    
    # Add columns
    for i, col in enumerate(columns):
        col_line = f" {col['name']} {col['type']}"
        if col['not_null']:
            col_line += f" {col['not_null']}"
        if col['comment']:
            col_line += col['comment']
        if i < len(columns) - 1:
            col_line += ","
        sf_ddl.append(col_line)
    
    sf_ddl.append(");")
    sf_ddl.append("")
    
    return schema_name, '\n'.join(sf_ddl)

def main():
    print("Converting Teradata tables to Snowflake Iceberg format...")
    
    # Read the input file
    with open('tmp5-tbl.sql', 'r') as f:
        content = f.read()
    
    # Split into individual table DDLs by looking for comment headers
    table_ddls = []
    lines = content.split('\n')
    current_table = []
    
    for line in lines:
        if line.startswith('-- ') and ' -> ' in line:
            # Start of a new table
            if current_table:
                table_ddls.append('\n'.join(current_table))
                current_table = []
        current_table.append(line)
    
    # Don't forget the last table
    if current_table:
        table_ddls.append('\n'.join(current_table))
    
    print(f"Found {len(table_ddls)} table DDL blocks to convert")
    
    # Convert each table
    converted_tables = []
    schemas = set()
    failed_count = 0
    
    for i, table_ddl in enumerate(table_ddls):
        result = convert_teradata_table_to_iceberg(table_ddl)
        if result:
            schema_name, sf_ddl = result
            schemas.add(schema_name)
            converted_tables.append(sf_ddl)
        else:
            failed_count += 1
    
    print(f"Successfully converted: {len(converted_tables)} tables")
    print(f"Failed to convert: {failed_count} tables")
    
    if not converted_tables:
        print("No tables were successfully converted. Check the input format.")
        return
    
    # Generate output file
    output_lines = []
    
    # Header
    output_lines.append("use role ACCOUNTADMIN;")
    output_lines.append("USE DATABASE PS_CLD_RW;")
    output_lines.append("")
    
    # Create schemas
    for schema in sorted(schemas):
        output_lines.append(f'drop schema if exists PS_CLD_RW."{schema.lower()}";')
        output_lines.append(f"drop schema if exists PS_CLD_RW.{schema};")
        output_lines.append(f"CREATE SCHEMA IF NOT EXISTS PS_CLD_RW.{schema};")
        output_lines.append("")
    
    # Add DROP statements for all tables
    drop_statements = []
    for table_ddl in converted_tables:
        # Extract table name from CREATE ICEBERG TABLE line
        create_match = re.search(r'CREATE ICEBERG TABLE (PS_CLD_RW\.[A-Z_]+\.[A-Z_]+)', table_ddl)
        if create_match:
            full_table_name = create_match.group(1)
            drop_statements.append(f"DROP TABLE IF EXISTS {full_table_name};")
    
    if drop_statements:
        output_lines.extend(drop_statements)
        output_lines.append("")
        output_lines.append("")
    
    # Add converted tables
    output_lines.extend(converted_tables)
    
    # Write output file
    with open('tmp5-ibrgTbl.sql', 'w') as f:
        f.write('\n'.join(output_lines))
    
    print(f"Conversion complete!")
    print(f"- Created {len(schemas)} schemas: {', '.join(sorted(schemas))}")
    print(f"- Converted {len(converted_tables)} tables")
    print(f"- Output written to: tmp5-ibrgTbl.sql")

if __name__ == "__main__":
    main() 