#!/usr/bin/env python3
"""
Extract Table Dependencies from BTEQ Scripts using SQLGlot

This script analyzes all BTEQ files to extract table references that will need
to exist in the catalog-linked database for dbt models to work properly.
"""

import json
import os
import re
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Set, Tuple

import sqlglot
from sqlglot import exp

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from services.parsing.bteq.lexer import lex_bteq
from services.preprocessing.substitution.service import SubstitutionService


class TableDependencyExtractor:
    """Extract table dependencies from BTEQ scripts using SQLGlot."""
    
    def __init__(self, bteq_directory: str, config_file: str = None):
        self.bteq_directory = Path(bteq_directory)
        self.config_file = config_file or "config/database/variable_substitution.cfg"
        
        # Initialize substitution service for variable replacement
        self.substitution_service = SubstitutionService(self.config_file)
        
        # Storage for results
        self.table_references: Dict[str, Set[str]] = defaultdict(set)
        self.schema_references: Dict[str, Set[str]] = defaultdict(set)
        self.database_references: Dict[str, Set[str]] = defaultdict(set)
        self.file_dependencies: Dict[str, Dict[str, List[str]]] = {}
        self.parsing_errors: Dict[str, List[str]] = defaultdict(list)
        
        # Separate tracking for SQLGlot vs Regex results
        self.sqlglot_results: Dict[str, Set[Tuple[str, str, str]]] = defaultdict(set)
        self.regex_results: Dict[str, Set[Tuple[str, str, str]]] = defaultdict(set)
        self.sqlglot_failures: Dict[str, str] = {}
        
    def extract_tables_from_sql(self, sql_text: str, file_name: str) -> Set[Tuple[str, str, str]]:
        """
        Extract table references from SQL using SQLGlot first, then regex fallback.
        Returns set of (database, schema, table) tuples.
        Tracks SQLGlot vs regex results separately.
        """
        sqlglot_tables = set()
        regex_tables = set()
        
        # Try SQLGlot first
        try:
            # Parse SQL with Teradata dialect
            parsed = sqlglot.parse_one(sql_text, dialect="teradata")
            
            # Find all table references, but filter out aliases and column references
            for table_node in parsed.find_all(exp.Table):
                # Skip if this looks like an alias (single word with no dots)
                table_name = str(table_node)
                
                # Only process if it has schema qualification or looks like a real table
                if ('.' in table_name and not table_name.count('.') > 2) or \
                   (not '.' in table_name and len(table_name) > 3 and '_' in table_name):
                    
                    # Extract database, schema, table parts
                    parts = []
                    if table_node.catalog:
                        parts.append(str(table_node.catalog))
                    if table_node.db:
                        parts.append(str(table_node.db))
                    if table_node.name:
                        parts.append(str(table_node.name))
                    
                    # Pad with None for missing parts
                    while len(parts) < 3:
                        parts.insert(0, None)
                        
                    database, schema, table = parts[-3], parts[-2], parts[-1]
                    
                    # Additional filtering - skip obvious aliases/columns
                    if table and schema:
                        # Skip if schema is a common alias pattern (single letters, DT1, etc.)
                        if not re.match(r'^[A-Z]{1,3}[0-9]?$', schema) and \
                           not schema.upper() in ['A', 'B', 'C', 'DT1', 'DT2', 'DT3', 'ADJ', 'GRC']:
                            sqlglot_tables.add((database, schema, table))
                        # Include if it looks like a real schema (PVTECH, PDDSTG, etc.)
                        elif schema.upper() in ['PVTECH', 'PDDSTG', 'UTILSTG', 'STAR_CAD_PROD_DATA', 
                                              'STAR_CAD_PROD_MACRO', 'PVCBODS', 'PVPATY', 'PUCB', 'DGRDDB']:
                            sqlglot_tables.add((database, schema, table))
                    elif table and not schema:
                        # For unqualified tables, only include if they look like real table names
                        if '_' in table and len(table) > 8:  # Real tables usually have underscores and are longer
                            sqlglot_tables.add((database, schema, table))
            
            # Store SQLGlot results
            self.sqlglot_results[file_name].update(sqlglot_tables)
                            
        except Exception as e:
            # Store SQLGlot failure reason
            self.sqlglot_failures[file_name] = str(e)
            print(f"SQLGlot parsing failed for {file_name}: {e}")
        
        # Always try regex as well for comparison
        regex_tables = self._regex_table_extraction(sql_text)
        self.regex_results[file_name].update(regex_tables)
        
        # Return combined results
        return sqlglot_tables.union(regex_tables)
    
    def _regex_table_extraction(self, sql_text: str) -> Set[Tuple[str, str, str]]:
        """Fallback regex-based table extraction for when SQLGlot fails."""
        tables = set()
        
        # Look for table references in FROM and JOIN clauses specifically
        # This avoids column references and focuses on actual tables
        sql_upper = sql_text.upper()
        
        # Pattern for FROM/JOIN followed by qualified table names
        from_join_pattern = r'(?:FROM|JOIN)\s+([A-Z_][A-Z0-9_]*(?:\.[A-Z_][A-Z0-9_]*){1,2})'
        matches = re.findall(from_join_pattern, sql_text, re.IGNORECASE | re.MULTILINE)
        
        for match in matches:
            parts = match.split('.')
            while len(parts) < 3:
                parts.insert(0, None)
            database, schema, table = parts[-3], parts[-2], parts[-1]
            
            # Filter out obvious aliases
            if schema and not re.match(r'^[A-Z]{1,3}[0-9]?$', schema):
                tables.add((database, schema, table))
        
        # Also look for variable patterns like %%CAD_PROD_DATA%%.TABLE_NAME
        var_pattern = r'%%([A-Z_][A-Z0-9_]*)%%\.([A-Z_][A-Z0-9_]*)'
        var_matches = re.findall(var_pattern, sql_text)
        for var_name, table_name in var_matches:
            tables.add((None, f"%%{var_name}%%", table_name))
        
        return tables
    
    def process_bteq_file(self, bteq_file: Path) -> Dict[str, any]:
        """Process a single BTEQ file and extract table dependencies."""
        print(f"Processing {bteq_file.name}...")
        
        try:
            # Read BTEQ file
            with open(bteq_file, 'r', encoding='utf-8') as f:
                bteq_content = f.read()
                
            # Apply variable substitution
            substituted_content, _ = self.substitution_service.substitute_variables(bteq_content)
            
            # Parse with BTEQ lexer
            parsed_result = lex_bteq(substituted_content)
            
            file_tables = set()
            file_errors = []
            
            # Extract tables from each SQL block
            for sql_block in parsed_result.sql_blocks:
                try:
                    tables = self.extract_tables_from_sql(sql_block.sql, bteq_file.name)
                    file_tables.update(tables)
                    
                    # Store by schema for analysis
                    for db, schema, table in tables:
                        full_name = '.'.join(filter(None, [db, schema, table]))
                        self.table_references[bteq_file.name].add(full_name)
                        
                        if schema:
                            self.schema_references[schema].add(table)
                        if db:
                            self.database_references[db].add(f"{schema}.{table}" if schema else table)
                            
                except Exception as e:
                    error_msg = f"Error parsing SQL block: {str(e)}"
                    file_errors.append(error_msg)
                    self.parsing_errors[bteq_file.name].append(error_msg)
            
            # Store file-level dependencies
            self.file_dependencies[bteq_file.name] = {
                'tables': list(file_tables),
                'table_count': len(file_tables),
                'sql_blocks': len(parsed_result.sql_blocks),
                'control_statements': len(parsed_result.controls),
                'errors': file_errors
            }
            
            return self.file_dependencies[bteq_file.name]
            
        except Exception as e:
            error_msg = f"Error processing file {bteq_file.name}: {str(e)}"
            self.parsing_errors[bteq_file.name].append(error_msg)
            return {'error': error_msg}
    
    def analyze_all_files(self) -> Dict[str, any]:
        """Analyze all BTEQ files in the directory."""
        print(f"Analyzing BTEQ files in {self.bteq_directory}")
        
        bteq_files = list(self.bteq_directory.glob("*.bteq"))
        print(f"Found {len(bteq_files)} BTEQ files")
        
        for bteq_file in bteq_files:
            self.process_bteq_file(bteq_file)
        
        return self.generate_summary()
    
    def generate_summary(self) -> Dict[str, any]:
        """Generate comprehensive summary of table dependencies."""
        
        # Collect unique tables across all files
        all_tables = set()
        for tables in self.table_references.values():
            all_tables.update(tables)
        
        # Group by schema
        schema_summary = {}
        for schema, tables in self.schema_references.items():
            schema_summary[schema] = {
                'table_count': len(tables),
                'tables': sorted(list(tables))
            }
        
        # Database summary
        database_summary = {}
        for database, tables in self.database_references.items():
            database_summary[database] = {
                'table_count': len(tables),
                'tables': sorted(list(tables))
            }
        
        # SQLGlot vs Regex comparison
        sqlglot_all_tables = set()
        regex_all_tables = set()
        
        for file_name, tables in self.sqlglot_results.items():
            for db, schema, table in tables:
                full_name = '.'.join(filter(None, [db, schema, table]))
                sqlglot_all_tables.add(full_name)
        
        for file_name, tables in self.regex_results.items():
            for db, schema, table in tables:
                full_name = '.'.join(filter(None, [db, schema, table]))
                regex_all_tables.add(full_name)
        
        # Format SQLGlot and regex results for comparison
        sqlglot_formatted = {}
        for file_name, tables in self.sqlglot_results.items():
            sqlglot_formatted[file_name] = ['.'.join(filter(None, [db, schema, table])) for db, schema, table in tables]
            
        regex_formatted = {}
        for file_name, tables in self.regex_results.items():
            regex_formatted[file_name] = ['.'.join(filter(None, [db, schema, table])) for db, schema, table in tables]
        
        return {
            'summary': {
                'total_files_processed': len(self.file_dependencies),
                'total_unique_tables': len(all_tables),
                'total_schemas': len(self.schema_references),
                'total_databases': len(self.database_references),
                'files_with_errors': len(self.parsing_errors),
                'sqlglot_successes': len(self.sqlglot_results),
                'sqlglot_failures': len(self.sqlglot_failures),
                'sqlglot_unique_tables': len(sqlglot_all_tables),
                'regex_unique_tables': len(regex_all_tables)
            },
            'all_tables': sorted(list(all_tables)),
            'by_schema': schema_summary,
            'by_database': database_summary,
            'file_dependencies': self.file_dependencies,
            'parsing_errors': dict(self.parsing_errors),
            'method_comparison': {
                'sqlglot_results': sqlglot_formatted,
                'regex_results': regex_formatted,
                'sqlglot_failures': self.sqlglot_failures,
                'sqlglot_only': sorted(list(sqlglot_all_tables - regex_all_tables)),
                'regex_only': sorted(list(regex_all_tables - sqlglot_all_tables)),
                'both_methods': sorted(list(sqlglot_all_tables & regex_all_tables))
            }
        }
    
    def save_results(self, output_file: str = "table_dependencies.json"):
        """Save results to JSON file."""
        results = self.generate_summary()
        
        output_path = Path(output_file)
        with open(output_path, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        
        print(f"Results saved to {output_path}")
        return results
    
    def print_summary(self):
        """Print a human-readable summary."""
        results = self.generate_summary()
        summary = results['summary']
        
        print("\n" + "="*60)
        print("TABLE DEPENDENCY ANALYSIS SUMMARY")
        print("="*60)
        print(f"Files processed: {summary['total_files_processed']}")
        print(f"Unique tables found: {summary['total_unique_tables']}")
        print(f"Schemas referenced: {summary['total_schemas']}")
        print(f"Databases referenced: {summary['total_databases']}")
        print(f"Files with parsing errors: {summary['files_with_errors']}")
        
        print(f"\nTOP SCHEMAS BY TABLE COUNT:")
        schema_counts = [(schema, data['table_count']) 
                        for schema, data in results['by_schema'].items()]
        for schema, count in sorted(schema_counts, key=lambda x: x[1], reverse=True)[:10]:
            print(f"  {schema}: {count} tables")
        
        print(f"\nALL UNIQUE TABLES:")
        for table in sorted(results['all_tables'][:20]):  # Show first 20
            print(f"  {table}")
        if len(results['all_tables']) > 20:
            print(f"  ... and {len(results['all_tables']) - 20} more")
        
        if results['parsing_errors']:
            print(f"\nFILES WITH ERRORS:")
            for file, errors in results['parsing_errors'].items():
                print(f"  {file}: {len(errors)} errors")


def main():
    """Main execution function."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Extract table dependencies from BTEQ scripts")
    parser.add_argument("--bteq-dir", default="references/current_state/bteq_bteq",
                       help="Directory containing BTEQ files")
    parser.add_argument("--config", default="config/database/variable_substitution.cfg",
                       help="Variable substitution config file")
    parser.add_argument("--output", default="table_dependencies.json",
                       help="Output JSON file")
    parser.add_argument("--quiet", action="store_true",
                       help="Suppress detailed output")
    
    args = parser.parse_args()
    
    # Create extractor
    extractor = TableDependencyExtractor(args.bteq_dir, args.config)
    
    # Run analysis
    extractor.analyze_all_files()
    
    # Save results
    results = extractor.save_results(args.output)
    
    # Print summary unless quiet
    if not args.quiet:
        extractor.print_summary()
    
    return results


if __name__ == "__main__":
    main()
