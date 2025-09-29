#!/usr/bin/env python3
"""CLI for the BTEQ parser service."""
from __future__ import annotations

import sys
import json
import argparse
from typing import Optional

from .parser_service import ParserService


def _read_all(path: Optional[str]) -> str:
    if path is None or path == "-":
        return sys.stdin.read()
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def main(argv: Optional[list[str]] = None) -> int:
    argv = argv if argv is not None else sys.argv[1:]
    p = argparse.ArgumentParser(description="BTEQ parser service CLI")
    p.add_argument("file", nargs="?", default="-", help="Input BTEQ .sql file path or - for stdin")
    p.add_argument("--verbose", "-v", action="store_true", help="Show both original and transpiled SQL")
    p.add_argument("--format", choices=["json", "readable"], default="json", help="Output format")
    p.add_argument("--analysis", "-a", action="store_true", help="Include advanced SQLGlot analysis")
    p.add_argument("--no-analysis", action="store_true", help="Disable advanced analysis for faster parsing")
    args = p.parse_args(argv)

    text = _read_all(args.file)
    enable_analysis = not args.no_analysis
    service = ParserService(enable_advanced_analysis=enable_analysis)
    out = service.parse(text)

    if args.format == "readable":
        _print_readable(out, args.verbose)
    else:
        payload = {
            "controls": [
                {
                    "type": c.type.name,
                    "line": c.line_no,
                    "raw": c.raw,
                }
                for c in out.controls
            ],
            "sql": [
                {
                    "start_line": s.start_line,
                    "end_line": s.end_line,
                    "original_sql": s.original if args.verbose else None,
                    "error": s.error,
                    "snowflake_sql": s.snowflake_sql,
                    "metadata": {k: list(v) for k, v in s.metadata.items()} if args.analysis and s.metadata else None,
                    "complexity_metrics": s.complexity_metrics if args.analysis else None,
                    "teradata_features": s.teradata_features if args.analysis else None,
                    "syntax_validation": s.syntax_validation if args.analysis else None,
                    "optimized_sql": s.optimized_sql if args.analysis else None,
                }
                for s in out.sql
            ],
        }
        print(json.dumps(payload, indent=2))
    return 0


def _print_readable(result, show_original: bool = False):
    """Print results in a human-readable format."""
    print("=" * 80)
    print("BTEQ PARSER RESULTS")
    print("=" * 80)
    
    print(f"\n📋 CONTROL STATEMENTS ({len(result.controls)} found):")
    print("-" * 50)
    for i, control in enumerate(result.controls, 1):
        print(f"{i:2d}. Line {control.line_no:3d}: {control.type.name:12s} | {control.raw}")
    
    print(f"\n🔍 SQL BLOCKS ({len(result.sql)} found):")
    print("-" * 50)
    for i, sql_block in enumerate(result.sql, 1):
        print(f"\n{i}. SQL Block (lines {sql_block.start_line}-{sql_block.end_line}):")
        
        if sql_block.error:
            print(f"   ❌ ERROR: {sql_block.error}")
        else:
            print("   ✅ Successfully transpiled")
        
        if show_original:
            print("\n   📝 ORIGINAL TERADATA SQL:")
            print("   " + "-" * 40)
            for line_no, line in enumerate(sql_block.original.split('\n'), sql_block.start_line):
                print(f"   {line_no:3d}| {line}")
        
        if sql_block.snowflake_sql:
            print(f"\n   ❄️  SNOWFLAKE SQL:")
            print("   " + "-" * 40)
            for line_no, line in enumerate(sql_block.snowflake_sql.split('\n'), 1):
                print(f"       | {line}")
        
        # Show advanced analysis if available
        if sql_block.metadata:
            print(f"\n   🔍 METADATA ANALYSIS:")
            print("   " + "-" * 40)
            for key, values in sql_block.metadata.items():
                if values:
                    print(f"   {key.upper()}: {', '.join(sorted(values))}")
        
        if sql_block.complexity_metrics:
            print(f"\n   📊 COMPLEXITY METRICS:")
            print("   " + "-" * 40)
            for metric, value in sql_block.complexity_metrics.items():
                print(f"   {metric.replace('_', ' ').title()}: {value}")
        
        if sql_block.teradata_features:
            print(f"\n   🎯 TERADATA-SPECIFIC FEATURES:")
            print("   " + "-" * 40)
            for feature in sql_block.teradata_features:
                print(f"   • {feature}")
        
        if sql_block.syntax_validation:
            print(f"\n   ✅ SYNTAX VALIDATION:")
            print("   " + "-" * 40)
            validation = sql_block.syntax_validation
            status = "✅ Valid" if validation.get("valid") else "❌ Invalid"
            print(f"   Status: {status}")
            if validation.get("error"):
                print(f"   Error: {validation['error']}")
        
        if sql_block.optimized_sql and sql_block.optimized_sql != sql_block.snowflake_sql:
            print(f"\n   🚀 OPTIMIZED SQL:")
            print("   " + "-" * 40)
            for line_no, line in enumerate(sql_block.optimized_sql.split('\n'), 1):
                print(f"       | {line}")
        
        print()
    
    print("=" * 80)


if __name__ == "__main__":
    raise SystemExit(main())
