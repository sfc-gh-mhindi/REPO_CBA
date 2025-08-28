#!/usr/bin/env python3
"""
Comprehensive BTEQ Analysis for Agentic Migration Solution
Analyzes all BTEQ files and generates a detailed markdown report for target state conversion.
"""

import os
import sys
import json
from pathlib import Path
from datetime import datetime

# Add the bteq_dcf module to the path
sys.path.insert(0, str(Path(__file__).parent / "bteq_dcf"))

from bteq_dcf.parser.parser_service import ParserService


def analyze_bteq_file(file_path: str, service: ParserService) -> dict:
    """Analyze a single BTEQ file and return structured results."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        result = service.parse(content)
        
        return {
            "file_path": file_path,
            "file_name": os.path.basename(file_path),
            "success": True,
            "controls": [
                {
                    "type": c.type.name,
                    "line": c.line_no,
                    "raw": c.raw,
                }
                for c in result.controls
            ],
            "sql_blocks": [
                {
                    "start_line": s.start_line,
                    "end_line": s.end_line,
                    "error": s.error,
                    "has_sql": s.snowflake_sql is not None,
                    "original_sql": s.original,
                    "snowflake_sql": s.snowflake_sql,
                    "metadata": {k: list(v) for k, v in s.metadata.items()} if s.metadata else {},
                    "complexity_metrics": s.complexity_metrics or {},
                    "teradata_features": s.teradata_features or [],
                    "syntax_validation": s.syntax_validation or {},
                    "syntax_valid": s.syntax_validation.get("valid", False) if s.syntax_validation else False,
                    "optimized_sql": s.optimized_sql,
                }
                for s in result.sql
            ]
        }
    except Exception as e:
        return {
            "file_path": file_path,
            "file_name": os.path.basename(file_path),
            "success": False,
            "error": str(e),
            "controls": [],
            "sql_blocks": []
        }


def generate_markdown_report(analysis_results: list) -> str:
    """Generate a comprehensive markdown report for agentic solution."""
    
    md = """# BTEQ Migration Analysis Report

## Executive Summary

This report provides a comprehensive analysis of all BTEQ files in the current state, designed to inform the agentic migration solution for converting Teradata BTEQ scripts to dbt models using the DCF (dbt Control Framework).

## Analysis Overview

"""
    
    # Summary statistics
    total_files = len(analysis_results)
    successful_files = sum(1 for r in analysis_results if r["success"])
    failed_files = total_files - successful_files
    
    total_controls = sum(len(r["controls"]) for r in analysis_results)
    total_sql_blocks = sum(len(r["sql_blocks"]) for r in analysis_results)
    
    md += f"""
### File Processing Summary
- **Total Files Analyzed**: {total_files}
- **Successfully Processed**: {successful_files}
- **Failed to Process**: {failed_files}
- **Total Control Statements**: {total_controls}
- **Total SQL Blocks**: {total_sql_blocks}

"""
    
    # Control statement analysis
    control_types = {}
    for result in analysis_results:
        for control in result["controls"]:
            control_type = control["type"]
            control_types[control_type] = control_types.get(control_type, 0) + 1
    
    md += """
### Control Statement Distribution
| Control Type | Count | Purpose |
|--------------|-------|---------|
"""
    
    control_descriptions = {
        "RUN": "Execute external scripts/files",
        "IF_ERRORCODE": "Error handling and flow control",
        "GOTO": "Jump to labeled sections",
        "LABEL": "Define jump targets",
        "LOGON": "Database connection",
        "LOGOFF": "Database disconnection",
        "IMPORT": "Import data from files",
        "EXPORT": "Export data to files",
        "OS_CMD": "Execute operating system commands",
        "COLLECT_STATS": "Collect table statistics",
        "CALL_SP": "Call stored procedures"
    }
    
    for control_type, count in sorted(control_types.items()):
        description = control_descriptions.get(control_type, "Unknown control type")
        md += f"| {control_type} | {count} | {description} |\n"
    
    # Teradata features analysis
    all_features = set()
    feature_counts = {}
    
    for result in analysis_results:
        for sql_block in result["sql_blocks"]:
            for feature in sql_block.get("teradata_features", []):
                all_features.add(feature)
                feature_counts[feature] = feature_counts.get(feature, 0) + 1
    
    md += f"""

### Teradata-Specific Features Requiring Migration Attention
| Feature | Occurrences | Migration Complexity |
|---------|-------------|---------------------|
"""
    
    complexity_map = {
        "Variable substitution": "Low - Replace with dbt variables",
        "QUALIFY clause": "Medium - Convert to window function + WHERE",
        "ROW_NUMBER() OVER": "Low - Direct Snowflake support",
        "ADD_MONTHS function": "Low - Use DATEADD in Snowflake",
        "EXTRACT with complex syntax": "Medium - Simplify extraction logic",
        "YEAR(4) TO MONTH intervals": "High - Complex date arithmetic conversion",
        "Teradata date arithmetic": "Medium - Convert to Snowflake date functions",
        "Teradata COLLECT STATS": "Low - Remove or replace with Snowflake equivalents"
    }
    
    for feature, count in sorted(feature_counts.items(), key=lambda x: x[1], reverse=True):
        complexity = complexity_map.get(feature, "Unknown complexity")
        md += f"| {feature} | {count} | {complexity} |\n"
    
    # Complexity analysis
    md += """

### SQL Complexity Analysis

The following metrics help determine the appropriate dbt materialization strategy:

"""
    
    complexity_stats = {
        "total_nodes": [],
        "select_count": [],
        "join_count": [],
        "subquery_count": [],
        "case_statements": [],
        "window_functions": [],
        "aggregate_functions": []
    }
    
    for result in analysis_results:
        for sql_block in result["sql_blocks"]:
            metrics = sql_block.get("complexity_metrics", {})
            for key in complexity_stats:
                if key in metrics:
                    complexity_stats[key].append(metrics[key])
    
    md += "| Metric | Min | Max | Avg | Total |\n"
    md += "|--------|-----|-----|-----|-------|\n"
    
    for metric, values in complexity_stats.items():
        if values:
            min_val = min(values)
            max_val = max(values)
            avg_val = sum(values) / len(values)
            total_val = sum(values)
            metric_name = metric.replace('_', ' ').title()
            md += f"| {metric_name} | {min_val} | {max_val} | {avg_val:.1f} | {total_val} |\n"
    
    # File-by-file analysis
    md += """

## Detailed File Analysis

### Migration Recommendations by File

"""
    
    for result in analysis_results:
        file_name = result["file_name"]
        md += f"""
#### {file_name}

"""
        
        if not result["success"]:
            md += f"❌ **FAILED TO PROCESS**: {result.get('error', 'Unknown error')}\n\n"
            continue
        
        # Determine file type and complexity
        control_count = len(result["controls"])
        sql_count = len(result["sql_blocks"])
        
        # Calculate total complexity
        total_complexity = 0
        teradata_features = set()
        
        for sql_block in result["sql_blocks"]:
            metrics = sql_block.get("complexity_metrics", {})
            total_complexity += metrics.get("total_nodes", 0)
            teradata_features.update(sql_block.get("teradata_features", []))
        
        # Determine migration strategy
        if sql_count == 0:
            strategy = "**Control-only script** - Convert to dbt macro or pre/post-hook"
        elif total_complexity < 50:
            strategy = "**Simple transformation** - Standard dbt model with table materialization"
        elif total_complexity < 200:
            strategy = "**Medium complexity** - Incremental model with DCF hooks recommended"
        else:
            strategy = "**High complexity** - Break into multiple models, use DCF full framework"
        
        md += f"""
- **Control Statements**: {control_count}
- **SQL Blocks**: {sql_count}
- **Total Complexity Score**: {total_complexity}
- **Teradata Features**: {len(teradata_features)}
- **Migration Strategy**: {strategy}

"""
        
        if teradata_features:
            md += "**Teradata Features Detected**:\n"
            for feature in sorted(teradata_features):
                md += f"- {feature}\n"
            md += "\n"
        
        # Control flow analysis
        if result["controls"]:
            md += "**Control Flow**:\n"
            for i, control in enumerate(result["controls"], 1):
                md += f"{i}. Line {control['line']}: `{control['type']}` - {control['raw'][:60]}{'...' if len(control['raw']) > 60 else ''}\n"
            md += "\n"
    
    # DCF mapping recommendations
    md += """

## DCF (dbt Control Framework) Mapping Recommendations

Based on the analysis, here are the recommended DCF patterns for migration:

### 1. Error Handling Patterns
- **BTEQ `.IF ERRORCODE`** → **DCF error handling macros**
- **BTEQ `.GOTO EXITERR`** → **DCF `check_error_and_end_prcs` macro**

### 2. Data Loading Patterns
- **Simple INSERT statements** → **dbt models with `table` materialization**
- **Complex INSERT with business logic** → **dbt models with `incremental` materialization + DCF hooks**
- **DELETE statements** → **Pre-hooks or separate dbt operations**

### 3. Stored Procedure Calls
- **CALL statements** → **DCF stored procedure macros or post-hooks**

### 4. File Operations
- **IMPORT/EXPORT** → **dbt seeds or external table references**

### 5. Statistics Collection
- **COLLECT STATS** → **Post-hooks with Snowflake ANALYZE TABLE equivalents**

## Next Steps for Agentic Solution

1. **Pattern Classification**: Use this analysis to train the classifier service on BTEQ patterns
2. **Template Generation**: Create dbt model templates based on complexity and feature analysis
3. **DCF Integration**: Map identified patterns to appropriate DCF macros and materializations
4. **Validation Rules**: Implement checks for successful conversion of identified Teradata features

---

*This report was generated automatically by the BTEQ Parser Service with advanced SQLGlot analysis.*
"""
    
    return md


def generate_individual_analysis(result: dict) -> str:
    """Generate individual file analysis markdown."""
    file_name = result["file_name"]
    
    md = f"""# {file_name} - BTEQ Analysis

## File Overview
- **File Name**: {file_name}
- **Analysis Status**: {'✅ Success' if result['success'] else '❌ Failed'}
- **Control Statements**: {len(result.get('controls', []))}
- **SQL Blocks**: {len(result.get('sql_blocks', []))}

"""
    
    if not result["success"]:
        md += f"""## Error Details
❌ **Processing Failed**: {result.get('error', 'Unknown error')}

This file requires manual review and potentially different parsing approach.
"""
        return md
    
    # Control flow analysis
    if result["controls"]:
        md += """## Control Flow Analysis

| Line | Type | Statement |
|------|------|-----------|
"""
        for control in result["controls"]:
            truncated = control['raw'][:80] + ('...' if len(control['raw']) > 80 else '')
            md += f"| {control['line']} | {control['type']} | `{truncated}` |\n"
    
    # SQL blocks analysis
    if result["sql_blocks"]:
        md += """
## SQL Blocks Analysis

"""
        for i, block in enumerate(result["sql_blocks"], 1):
            complexity = block.get("complexity_metrics", {})
            total_nodes = complexity.get("total_nodes", 0)
            validation = block.get('syntax_validation', {})
            
            md += f"""### SQL Block {i} (Lines {block['start_line']}-{block['end_line']})
- **Complexity Score**: {total_nodes}
- **Has Valid SQL**: {'✅' if block['has_sql'] else '❌'}
- **Conversion Successful**: {'✅' if block['snowflake_sql'] and not block.get('error') else '❌'}
- **Syntax Validation**: {'✅ Valid' if block['syntax_valid'] else '❌ Invalid'}
- **Teradata Features**: {len(block.get('teradata_features', []))}

"""
            
            # Show original Teradata SQL
            if block.get('original_sql'):
                md += "#### 📝 Original Teradata SQL:\n```sql\n"
                md += block['original_sql'].strip()
                md += "\n```\n\n"
            
            # Show converted Snowflake SQL
            if block.get('snowflake_sql'):
                md += "#### ❄️ Converted Snowflake SQL:\n```sql\n"
                md += block['snowflake_sql'].strip()
                md += "\n```\n\n"
            elif block.get('error'):
                md += "#### ❌ Conversion Failed:\n"
                md += f"**Error**: {block['error']}\n\n"
            
            # Show syntax validation details
            if validation:
                md += "#### 🔍 Syntax Validation Details:\n"
                md += f"- **Valid**: {'✅' if validation.get('valid', False) else '❌'}\n"
                if validation.get('parse_tree_depth'):
                    md += f"- **Parse Tree Depth**: {validation.get('parse_tree_depth')}\n"
                if validation.get('error_message'):
                    md += f"- **Error Message**: {validation.get('error_message')}\n"
                if validation.get('problematic_tokens'):
                    md += f"- **Problematic Tokens**: {', '.join(validation.get('problematic_tokens', []))}\n"
                md += "\n"
            
            # Show Teradata features
            if block.get('teradata_features'):
                md += "#### 🎯 Teradata Features Detected:\n"
                for feature in block['teradata_features']:
                    md += f"- {feature}\n"
                md += "\n"
            
            # Show optimized SQL if available
            if block.get('optimized_sql') and block['optimized_sql'] != block.get('original_sql'):
                md += "#### ⚡ Optimized SQL:\n```sql\n"
                md += block['optimized_sql'].strip()
                md += "\n```\n\n"
            
            # Show metadata if available
            metadata = block.get('metadata', {})
            if metadata and any(metadata.values()):
                md += "#### 📊 SQL Metadata:\n"
                for key, values in metadata.items():
                    if values:
                        # Convert values to strings in case they're not strings
                        str_values = [str(v) for v in values]
                        md += f"- **{key.title()}**: {', '.join(sorted(str_values))}\n"
                md += "\n"
    
    # Migration recommendations
    total_complexity = sum(
        block.get("complexity_metrics", {}).get("total_nodes", 0) 
        for block in result["sql_blocks"]
    )
    
    md += """## Migration Recommendations

"""
    
    if len(result["sql_blocks"]) == 0:
        strategy = "**Control-only script** - Convert to dbt macro or pre/post-hook"
    elif total_complexity < 50:
        strategy = "**Simple transformation** - Standard dbt model with table materialization"
    elif total_complexity < 200:
        strategy = "**Medium complexity** - Incremental model with DCF hooks recommended"
    else:
        strategy = "**High complexity** - Break into multiple models, use DCF full framework"
    
    md += f"""### Suggested Migration Strategy
{strategy}

### DCF Mapping
- **Error Handling**: Convert `.IF ERRORCODE` patterns to DCF error handling macros
- **File Operations**: Replace IMPORT/EXPORT with dbt seeds or external tables
- **Statistics**: Replace COLLECT STATS with Snowflake post-hooks
- **Stored Procedures**: Map CALL statements to DCF stored procedure macros

---

*Generated by BTEQ Parser Service - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*
"""
    
    return md


def generate_readme(analysis_results: list, output_dir: Path, timestamp: str) -> str:
    """Generate a comprehensive README file with summary table."""
    
    readme_content = f"""# BTEQ Analysis Results - {timestamp}

## Overview

This directory contains a comprehensive analysis of **{len(analysis_results)} BTEQ files** converted from Teradata to Snowflake SQL using the advanced BTEQ Parser Service with SQLGlot integration.

## Directory Structure

```
bteq_analysis_{timestamp}/
├── README.md                           # This file - summary overview
├── bteq_migration_analysis.md          # Comprehensive analysis report
├── analysis_summary.json              # Machine-readable summary data
└── individual/                        # Individual file analyses
    ├── [filename]_analysis.md          # Detailed analysis per file
    └── ...
```

## Analysis Summary

"""
    
    # Summary statistics
    successful = sum(1 for r in analysis_results if r["success"])
    failed = len(analysis_results) - successful
    total_controls = sum(len(r.get("controls", [])) for r in analysis_results)
    total_sql_blocks = sum(len(r.get("sql_blocks", [])) for r in analysis_results)
    
    readme_content += f"""
### 📊 Overall Statistics

| Metric | Value |
|--------|-------|
| **Total Files Analyzed** | {len(analysis_results)} |
| **Successfully Processed** | {successful} |
| **Failed to Process** | {failed} |
| **Total Control Statements** | {total_controls} |
| **Total SQL Blocks** | {total_sql_blocks} |

"""
    
    # File-by-file summary table
    readme_content += """## 📋 File Analysis Summary

| File Name | Status | Controls | SQL Blocks | Complexity | Features | Migration Strategy | SQL Types |
|-----------|--------|----------|------------|------------|----------|-------------------|-----------|
"""
    
    for result in sorted(analysis_results, key=lambda x: x["file_name"]):
        file_name = result["file_name"]
        status = "✅" if result["success"] else "❌"
        controls = len(result.get("controls", []))
        sql_blocks = len(result.get("sql_blocks", []))
        
        if not result["success"]:
            readme_content += f"| {file_name} | {status} | {controls} | {sql_blocks} | - | - | Manual Review | Error |\n"
            continue
            
        # Calculate total complexity
        total_complexity = sum(
            block.get("complexity_metrics", {}).get("total_nodes", 0) 
            for block in result["sql_blocks"]
        )
        
        # Count unique Teradata features
        all_features = set()
        for block in result["sql_blocks"]:
            all_features.update(block.get("teradata_features", []))
        
        features_count = len(all_features)
        
        # Determine migration strategy
        if sql_blocks == 0:
            strategy = "dbt Macro"
        elif total_complexity < 50:
            strategy = "Simple Model"
        elif total_complexity < 200:
            strategy = "Incremental Model"
        else:
            strategy = "Complex Model"
        
        # Determine SQL types from metadata
        sql_types = set()
        for block in result["sql_blocks"]:
            original_sql = block.get("original_sql", "").upper().strip()
            if original_sql.startswith("INSERT"):
                sql_types.add("INSERT")
            elif original_sql.startswith("UPDATE"):
                sql_types.add("UPDATE") 
            elif original_sql.startswith("DELETE"):
                sql_types.add("DELETE")
            elif original_sql.startswith("SELECT"):
                sql_types.add("SELECT")
            elif original_sql.startswith("CREATE"):
                sql_types.add("CREATE")
            elif original_sql.startswith("DROP"):
                sql_types.add("DROP")
            elif "MERGE" in original_sql:
                sql_types.add("MERGE")
            
        sql_types_str = ", ".join(sorted(sql_types)) if sql_types else "Other"
        
        readme_content += f"| {file_name} | {status} | {controls} | {sql_blocks} | {total_complexity} | {features_count} | {strategy} | {sql_types_str} |\n"
    
    # Control statement summary
    control_types = {}
    for result in analysis_results:
        for control in result.get("controls", []):
            control_type = control["type"]
            control_types[control_type] = control_types.get(control_type, 0) + 1
    
    readme_content += """
## 🎛️ Control Statement Analysis

| Control Type | Count | Migration Approach |
|--------------|-------|--------------------|
"""
    
    control_migration_map = {
        "RUN": "dbt pre-hook or external script",
        "IF_ERRORCODE": "DCF error handling macro", 
        "GOTO": "DCF flow control",
        "LABEL": "DCF checkpoint/label",
        "LOGON": "Remove (handled by dbt profiles)",
        "LOGOFF": "Remove (handled by dbt profiles)",
        "IMPORT": "dbt seeds or external tables",
        "EXPORT": "dbt post-hook or external process",
        "OS_CMD": "dbt post-hook or external process",
        "COLLECT_STATS": "Snowflake post-hook",
        "CALL_SP": "DCF stored procedure macro"
    }
    
    for control_type, count in sorted(control_types.items()):
        migration_approach = control_migration_map.get(control_type, "Manual review required")
        readme_content += f"| {control_type} | {count} | {migration_approach} |\n"
    
    # Teradata features analysis
    all_features = set()
    feature_counts = {}
    
    for result in analysis_results:
        for sql_block in result.get("sql_blocks", []):
            for feature in sql_block.get("teradata_features", []):
                all_features.add(feature)
                feature_counts[feature] = feature_counts.get(feature, 0) + 1
    
    if feature_counts:
        readme_content += """
## 🎯 Teradata Features Requiring Migration

| Feature | Occurrences | Conversion Status | Migration Notes |
|---------|-------------|-------------------|-----------------|
"""
        
        feature_notes = {
            "Variable substitution": "✅ Automatic | Replace with dbt variables",
            "QUALIFY clause": "⚠️ Manual | Convert to window function + WHERE",
            "ROW_NUMBER() OVER": "✅ Automatic | Direct Snowflake support",
            "ADD_MONTHS function": "✅ Automatic | Use DATEADD in Snowflake", 
            "EXTRACT with complex syntax": "⚠️ Manual | Simplify extraction logic",
            "YEAR(4) TO MONTH intervals": "❌ Manual | Complex date arithmetic conversion",
            "Teradata date arithmetic": "⚠️ Semi-Auto | Convert to Snowflake date functions",
            "Teradata COLLECT STATS": "✅ Automatic | Remove or replace with Snowflake equivalents"
        }
        
        for feature, count in sorted(feature_counts.items(), key=lambda x: x[1], reverse=True):
            notes = feature_notes.get(feature, "❓ Review | Manual analysis required")
            readme_content += f"| {feature} | {count} | {notes} |\n"
    
    # Failed files section
    failed_files = [r for r in analysis_results if not r["success"]]
    if failed_files:
        readme_content += """
## ⚠️ Files Requiring Manual Attention

The following files failed automated processing and require manual review:

| File Name | Error | Recommendation |
|-----------|-------|----------------|
"""
        for result in failed_files:
            error = result.get("error", "Unknown error")
            if "utf-8" in error:
                recommendation = "Check file encoding - may need conversion from EBCDIC/Latin-1"
            else:
                recommendation = "Review file structure and BTEQ syntax"
            readme_content += f"| {result['file_name']} | {error} | {recommendation} |\n"
    
    readme_content += f"""

## 🚀 Next Steps

1. **Review Individual Analysis**: Check `individual/` directory for detailed file-by-file analysis
2. **Prioritize Migration**: Start with Simple Models, then Incremental, then Complex
3. **DCF Integration**: Use control statement mappings for DCF macro implementation  
4. **Teradata Features**: Address high-occurrence features first for maximum impact
5. **Manual Review**: Handle failed files and complex manual conversion cases

## 📝 Notes

- **Complexity Score**: Based on SQL AST node count (higher = more complex)
- **Migration Strategy**: Recommended dbt approach based on complexity and features
- **SQL Types**: Primary SQL operation types found in each file
- **Features**: Count of unique Teradata-specific features requiring conversion

---

*Generated by BTEQ Parser Service - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*
"""
    
    return readme_content


def main():
    """Main execution function."""
    bteq_dir = "current_state/bteq_sql"
    
    # Create timestamped output directory
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = Path(f"bteq_dcf/analysis/reports/bteq_analysis_{timestamp}")
    individual_dir = output_dir / "individual"
    
    # Create directories
    output_dir.mkdir(parents=True, exist_ok=True)
    individual_dir.mkdir(parents=True, exist_ok=True)
    
    main_report_file = output_dir / "bteq_migration_analysis.md"
    summary_json = output_dir / "analysis_summary.json"
    readme_file = output_dir / "README.md"
    
    print(f"🔍 Starting comprehensive BTEQ analysis...")
    print(f"📁 Output directory: {output_dir}")
    
    # Initialize parser service with advanced analysis
    service = ParserService(enable_advanced_analysis=True)
    
    # Get all SQL files
    sql_files = []
    for file_name in os.listdir(bteq_dir):
        if file_name.endswith('.sql'):
            sql_files.append(os.path.join(bteq_dir, file_name))
    
    print(f"📁 Found {len(sql_files)} BTEQ files to analyze")
    
    # Analyze each file
    results = []
    for i, file_path in enumerate(sql_files, 1):
        file_name = os.path.basename(file_path)
        print(f"📊 Analyzing {i}/{len(sql_files)}: {file_name}")
        
        result = analyze_bteq_file(file_path, service)
        results.append(result)
        
        # Generate individual analysis file
        individual_md = generate_individual_analysis(result)
        individual_file = individual_dir / f"{file_name.replace('.sql', '_analysis.md')}"
        
        with open(individual_file, 'w', encoding='utf-8') as f:
            f.write(individual_md)
    
    # Generate comprehensive markdown report
    print("📝 Generating comprehensive markdown report...")
    markdown_content = generate_markdown_report(results)
    
    # Write main report
    with open(main_report_file, 'w', encoding='utf-8') as f:
        f.write(markdown_content)
    
    # Generate summary JSON for programmatic access
    summary_data = {
        "timestamp": datetime.now().isoformat(),
        "total_files": len(results),
        "successful_files": sum(1 for r in results if r["success"]),
        "failed_files": sum(1 for r in results if not r["success"]),
        "total_controls": sum(len(r.get("controls", [])) for r in results),
        "total_sql_blocks": sum(len(r.get("sql_blocks", [])) for r in results),
        "files": [
            {
                "name": r["file_name"],
                "success": r["success"],
                "controls": len(r.get("controls", [])),
                "sql_blocks": len(r.get("sql_blocks", [])),
                "error": r.get("error") if not r["success"] else None
            }
            for r in results
        ]
    }
    
    with open(summary_json, 'w', encoding='utf-8') as f:
        json.dump(summary_data, f, indent=2)
    
    # Generate and write README file
    print("📄 Generating README with summary table...")
    readme_content = generate_readme(results, output_dir, timestamp)
    
    with open(readme_file, 'w', encoding='utf-8') as f:
        f.write(readme_content)
    
    print(f"✅ Analysis complete!")
    print(f"📄 README: {readme_file}")
    print(f"📋 Main report: {main_report_file}")
    print(f"📊 Summary JSON: {summary_json}")
    print(f"📁 Individual analyses: {individual_dir}")
    print(f"📋 Summary: {len(results)} files analyzed")
    
    successful = sum(1 for r in results if r["success"])
    failed = len(results) - successful
    print(f"   - ✅ Successful: {successful}")
    print(f"   - ❌ Failed: {failed}")
    
    if failed > 0:
        print(f"\n⚠️  Failed files:")
        for result in results:
            if not result["success"]:
                print(f"   - {result['file_name']}: {result.get('error', 'Unknown error')}")


if __name__ == "__main__":
    main()
