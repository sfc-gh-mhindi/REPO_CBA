#!/usr/bin/env python3
"""
Split Table Extraction Results into SQLGlot and Regex JSON files

This script takes the combined extraction results and creates separate
JSON files for SQLGlot and regex-based table extraction results.
"""

import json
import sys
from pathlib import Path
from collections import defaultdict


def split_results(input_file: str = "final_comparison.json"):
    """Split combined results into separate SQLGlot and regex files."""
    
    with open(input_file, 'r') as f:
        data = json.load(f)
    
    # Extract method comparison data
    comparison = data['method_comparison']
    
    # Create SQLGlot-focused results
    sqlglot_results = {
        "extraction_method": "SQLGlot (Teradata dialect)",
        "description": "Tables extracted using SQLGlot AST parsing with Teradata dialect",
        "summary": {
            "total_files_analyzed": len(data['file_dependencies']),
            "files_successfully_parsed": len(comparison['sqlglot_results']),
            "files_failed_to_parse": len(comparison['sqlglot_failures']),
            "total_unique_tables": data['summary']['sqlglot_unique_tables'],
            "success_rate": f"{len(comparison['sqlglot_results']) / len(data['file_dependencies']) * 100:.1f}%"
        },
        "results_by_file": comparison['sqlglot_results'],
        "parsing_failures": comparison['sqlglot_failures'],
        "unique_tables": sorted(list({
            table for file_tables in comparison['sqlglot_results'].values() 
            for table in file_tables
        })),
        "tables_by_schema": {},
        "sqlglot_advantages": [
            "Proper SQL AST parsing",
            "Handles complex SQL constructs",
            "Identifies table references in context",
            "Can distinguish between tables and aliases"
        ],
        "sqlglot_limitations": [
            "Fails on Teradata-specific syntax",
            "Struggles with non-standard SQL patterns",
            "Cannot parse all legacy BTEQ constructs"
        ]
    }
    
    # Create regex-focused results
    regex_results = {
        "extraction_method": "Regex Pattern Matching",
        "description": "Tables extracted using regex patterns targeting FROM/JOIN clauses",
        "summary": {
            "total_files_analyzed": len(data['file_dependencies']),
            "files_successfully_parsed": len(comparison['regex_results']),
            "files_failed_to_parse": 0,  # Regex doesn't fail
            "total_unique_tables": data['summary']['regex_unique_tables'],
            "success_rate": "100.0%"
        },
        "results_by_file": comparison['regex_results'],
        "parsing_failures": {},
        "unique_tables": sorted(list({
            table for file_tables in comparison['regex_results'].values() 
            for table in file_tables
        })),
        "tables_by_schema": {},
        "regex_advantages": [
            "Works on all SQL syntax variants",
            "Never fails to process a file",
            "Catches edge cases SQLGlot misses",
            "Handles Teradata-specific patterns"
        ],
        "regex_limitations": [
            "May miss complex nested table references",
            "Could extract false positives from comments",
            "Less precise than AST-based parsing",
            "Relies on pattern recognition"
        ]
    }
    
    # Group tables by schema for both methods
    def group_by_schema(table_list):
        schema_groups = defaultdict(list)
        for table in table_list:
            if '.' in table:
                schema = table.split('.')[0]
                table_name = table.split('.', 1)[1]
                schema_groups[schema].append(table_name)
            else:
                schema_groups['unqualified'].append(table)
        
        return {
            schema: {
                'table_count': len(tables),
                'tables': sorted(tables)
            }
            for schema, tables in schema_groups.items()
        }
    
    sqlglot_results['tables_by_schema'] = group_by_schema(sqlglot_results['unique_tables'])
    regex_results['tables_by_schema'] = group_by_schema(regex_results['unique_tables'])
    
    # Add comparison metrics
    sqlglot_only = set(comparison['sqlglot_only'])
    regex_only = set(comparison['regex_only'])
    both_methods = set(comparison['both_methods'])
    
    sqlglot_results['comparison_metrics'] = {
        "found_by_sqlglot_only": len(sqlglot_only),
        "found_by_both_methods": len(both_methods),
        "sqlglot_exclusive_tables": sorted(list(sqlglot_only))
    }
    
    regex_results['comparison_metrics'] = {
        "found_by_regex_only": len(regex_only),
        "found_by_both_methods": len(both_methods),
        "regex_exclusive_tables": sorted(list(regex_only))
    }
    
    # Save separate files
    sqlglot_file = "table_extraction_sqlglot.json"
    regex_file = "table_extraction_regex.json"
    
    with open(sqlglot_file, 'w') as f:
        json.dump(sqlglot_results, f, indent=2)
    
    with open(regex_file, 'w') as f:
        json.dump(regex_results, f, indent=2)
    
    print(f"✅ Created {sqlglot_file}")
    print(f"✅ Created {regex_file}")
    
    # Print summary comparison
    print("\n" + "="*80)
    print("EXTRACTION METHOD COMPARISON")
    print("="*80)
    print(f"SQLGlot Results:")
    print(f"  Files parsed: {len(comparison['sqlglot_results'])}/{len(data['file_dependencies'])}")
    print(f"  Unique tables: {data['summary']['sqlglot_unique_tables']}")
    print(f"  Success rate: {len(comparison['sqlglot_results']) / len(data['file_dependencies']) * 100:.1f}%")
    
    print(f"\nRegex Results:")
    print(f"  Files parsed: {len(comparison['regex_results'])}/{len(data['file_dependencies'])}")
    print(f"  Unique tables: {data['summary']['regex_unique_tables']}")
    print(f"  Success rate: 100.0%")
    
    print(f"\nOverlap Analysis:")
    print(f"  Both methods found: {len(both_methods)} tables")
    print(f"  SQLGlot exclusive: {len(sqlglot_only)} tables")
    print(f"  Regex exclusive: {len(regex_only)} tables")
    
    return sqlglot_results, regex_results


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Split extraction results into SQLGlot and regex JSON files")
    parser.add_argument("--input", default="final_comparison.json",
                       help="Input combined results file")
    
    args = parser.parse_args()
    
    split_results(args.input)
