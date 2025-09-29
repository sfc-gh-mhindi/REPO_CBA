#!/usr/bin/env python3

import re
import os

def convert_teradata_view_to_snowflake(view_ddl):
    """Convert a single Teradata view DDL to Snowflake format"""
    
    # Replace REPLACE VIEW with CREATE OR REPLACE VIEW
    view_ddl = re.sub(r'\bREPLACE\s+VIEW\b', 'CREATE OR REPLACE VIEW', view_ddl, flags=re.IGNORECASE)
    view_ddl = re.sub(r'\bCREATE\s+VIEW\b', 'CREATE OR REPLACE VIEW', view_ddl, flags=re.IGNORECASE)
    
    # Remove Teradata-specific locking clauses
    view_ddl = re.sub(r'\s+AS\s+LOCKING\s+(?:TABLE|ROW|VIEW)\s+[^\s]+\s+FOR\s+ACCESS', ' AS', view_ddl, flags=re.IGNORECASE | re.MULTILINE)
    
    # Convert TD_SYSFNLIB.GETBIT to GETBIT
    view_ddl = re.sub(r'TD_SYSFNLIB\.GETBIT\s*\(', 'GETBIT(', view_ddl, flags=re.IGNORECASE)
    
    # Convert USERNAME = USER to UPPER(RTRIM(username)) = UPPER(RTRIM(CURRENT_USER))
    view_ddl = re.sub(r'\bUSERNAME\s*=\s*USER\b', 'UPPER(RTRIM(username)) = UPPER(RTRIM(CURRENT_USER))', view_ddl, flags=re.IGNORECASE)
    
    # Update table references to use proper database prefixes FIRST
    # Replace common schema references in FROM clauses
    schema_mappings = {
        'STAR_CAD_PROD_DATA': 'PS_CLD_RW.STAR_CAD_PROD_DATA',
        'P_P01_STAR_CAD_PROD_DATA': 'PS_CLD_RW.P_P01_STAR_CAD_PROD_DATA',
        'PDGRD': 'PS_CLD_RW.PDGRD',
        'PDDSTG': 'PS_CLD_RW.PDDSTG',
        'PVTECH': 'PS_GDW1_BTEQ.PVTECH',
        'P_P01_PVTECH': 'PS_GDW1_BTEQ.P_P01_PVTECH',
        'PVSECURITY': 'PS_GDW1_BTEQ.PVSECURITY',
        'P_P01_PVSECURITY': 'PS_GDW1_BTEQ.P_P01_PVSECURITY'
    }
    
    for old_schema, new_schema in schema_mappings.items():
        # Replace schema references in FROM, JOIN, and WHERE clauses
        pattern = r'\b' + old_schema + r'\.([A-Z_][A-Z0-9_]*)'
        replacement = new_schema + r'.\1'
        view_ddl = re.sub(pattern, replacement, view_ddl, flags=re.IGNORECASE)
    
    # Add PS_GDW1_BTEQ database prefix to view names AFTER table references are updated
    # This regex looks for CREATE OR REPLACE VIEW followed by schema.view_name
    view_ddl = re.sub(
        r'(CREATE\s+OR\s+REPLACE\s+VIEW\s+)([A-Z_][A-Z0-9_]*\.[A-Z_][A-Z0-9_]*)',
        r'\1PS_GDW1_BTEQ.\2',
        view_ddl,
        flags=re.IGNORECASE | re.MULTILINE
    )
    
    # Convert Teradata functions to Snowflake equivalents
    # SUBSTR to SUBSTRING (Snowflake prefers SUBSTRING)
    view_ddl = re.sub(r'\bSUBSTR\s*\(', 'SUBSTRING(', view_ddl, flags=re.IGNORECASE)
    
    # Current_Date to CURRENT_DATE
    view_ddl = re.sub(r'\bCurrent_Date\b', 'CURRENT_DATE', view_ddl, flags=re.IGNORECASE)
    
    # EXTRACT function format
    view_ddl = re.sub(r'\bEXTRACT\s*\(\s*DAY\s+from\s+CURRENT_DATE\s*\)', 'EXTRACT(DAY FROM CURRENT_DATE)', view_ddl, flags=re.IGNORECASE)
    
    # ADD_MONTHS to DATEADD
    view_ddl = re.sub(
        r'ADD_MONTHS\s*\(\s*([^,]+)\s*,\s*([^)]+)\s*\)',
        r'DATEADD(MONTH, \2, \1)',
        view_ddl,
        flags=re.IGNORECASE
    )
    
    # Clean up any double database prefixes that may have been created
    view_ddl = re.sub(r'PS_GDW1_BTEQ\.PS_GDW1_BTEQ\.', 'PS_GDW1_BTEQ.', view_ddl)
    view_ddl = re.sub(r'PS_CLD_RW\.PS_CLD_RW\.', 'PS_CLD_RW.', view_ddl)
    
    return view_ddl

def convert_views_file():
    """Convert all views in tmp6-views.sql to Snowflake format"""
    
    print("Reading tmp6-views.sql...")
    
    with open('tmp6-views.sql', 'r') as f:
        content = f.read()
    
    print("Converting views to Snowflake format...")
    
    # Split the content by view boundaries (looking for view comments)
    view_sections = re.split(r'(-- [A-Z_]+\.[A-Z_]+ -> [A-Z_]+\.[A-Z_]+)', content)
    
    converted_content = []
    converted_content.append("-- Snowflake Views converted from Teradata\n")
    converted_content.append("-- Generated automatically\n\n")
    converted_content.append("USE DATABASE PS_GDW1_BTEQ;\n\n")
    
    for i, section in enumerate(view_sections):
        if section.strip():
            if section.startswith('--') and '->' in section:
                # This is a comment header
                converted_content.append(section + "\n")
            elif 'CREATE VIEW' in section.upper() or 'REPLACE VIEW' in section.upper():
                # This is a view DDL
                converted_view = convert_teradata_view_to_snowflake(section)
                converted_content.append(converted_view + "\n")
            else:
                # Other content (comments, etc.)
                converted_content.append(section)
    
    # Write the converted content
    output_file = 'tmp6-SF Views.sql'
    with open(output_file, 'w') as f:
        f.write(''.join(converted_content))
    
    print(f"✅ Conversion completed!")
    print(f"📁 Output file: {output_file}")
    
    # Count views
    view_count = len(re.findall(r'CREATE\s+OR\s+REPLACE\s+VIEW', ''.join(converted_content), re.IGNORECASE))
    print(f"📊 Converted {view_count} views to Snowflake format")
    
    # Verify no double prefixes remain
    double_prefixes = re.findall(r'PS_GDW1_BTEQ\.PS_GDW1_BTEQ\.|PS_CLD_RW\.PS_CLD_RW\.', ''.join(converted_content))
    if double_prefixes:
        print(f"⚠️  Warning: Found {len(double_prefixes)} double prefixes that need manual review")
    else:
        print("✅ No double prefixes detected")

if __name__ == "__main__":
    convert_views_file() 