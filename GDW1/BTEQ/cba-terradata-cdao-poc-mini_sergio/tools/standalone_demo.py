#!/usr/bin/env python3
"""Standalone demo script with no external imports."""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

# Direct imports to avoid dependency issues
sys.path.insert(0, str(project_root / "bteq_dcf"))

from parser.tokens import ParserResult, ControlStatement, ControlType, SqlBlock
from parser.bteq_lexer import lex_bteq
from services.conversion.stored_proc.generator import SnowflakeSPGenerator


def standalone_demo():
    """Demonstrate the BTEQ generator with a standalone example."""
    
    print("="*80)
    print("BTEQ TO SNOWFLAKE STORED PROCEDURE GENERATOR - STANDALONE DEMO")
    print("="*80)
    
    # Try to load a real BTEQ file for a more comprehensive demo
    bteq_file = project_root / "current_state" / "bteq_bteq" / "ACCT_BALN_BKDT_ADJ_RULE_ISRT.bteq"
    
    if bteq_file.exists():
        print(f"📖 Loading real BTEQ file: {bteq_file.name}")
        with open(bteq_file, 'r', encoding='utf-8', errors='ignore') as f:
            sample_bteq = f.read()
        print(f"   File size: {len(sample_bteq)} characters")
    else:
        print(f"📖 Using sample BTEQ script (real file not found)")
        sample_bteq = """
.RUN FILE=%%BTEQ_LOGON_SCRIPT%%
.IF ERRORCODE <> 0 THEN .GOTO EXITERR

-- Sample BTEQ script for account balance processing
DELETE %%DDSTG%%.ACCT_BALN_BKDT_ADJ_RULE;

.IF ERRORCODE <> 0 THEN .GOTO EXITERR

INSERT INTO %%DDSTG%%.ACCT_BALN_BKDT_ADJ_RULE
(
    ACCT_ID,
    BALN_AMT,
    ADJ_AMT,
    EFCT_DT
)
SELECT 
    A.ACCT_ID,
    A.CURR_BALN_AMT,
    B.ADJ_AMT,
    CURRENT_DATE
FROM %%DDSTG%%.ACCT_BALN A
INNER JOIN %%DDSTG%%.ACCT_ADJ B
    ON A.ACCT_ID = B.ACCT_ID
WHERE A.EFCT_DT <= CURRENT_DATE
  AND B.ADJ_TYPE = 'BKDT';

.IF ERRORCODE <> 0 THEN .GOTO EXITERR

COLLECT STATS ON %%DDSTG%%.ACCT_BALN_BKDT_ADJ_RULE;

CALL SP_UPDATE_PROCESS_STATUS(%%PROCESS_KEY%%, 'COMPLETED');

.IF ERRORCODE <> 0 THEN .GOTO EXITERR

.LABEL EXITERR
.LOGOFF
"""
    
    # Show first few lines of BTEQ
    print(f"\n📝 BTEQ Content Preview (first 15 lines):")
    print("-" * 60)
    bteq_lines = sample_bteq.strip().split('\n')
    for i, line in enumerate(bteq_lines[:15], 1):
        print(f"{i:2d}| {line}")
    if len(bteq_lines) > 15:
        print(f"    ... ({len(bteq_lines) - 15} more lines)")
    
    # Parse BTEQ
    print(f"\n🔍 Parsing BTEQ content...")
    parser_result = lex_bteq(sample_bteq)
    
    print(f"   Total lines processed: {len(bteq_lines)}")
    print(f"   Control statements found: {len(parser_result.controls)}")
    
    # Show control statements
    if parser_result.controls:
        print(f"\n   📋 Control Statements:")
        for i, control in enumerate(parser_result.controls, 1):
            print(f"     {i:2d}. Line {control.line_no:3d}: {control.type.name:15s} | {control.raw[:50]}")
            if len(control.raw) > 50:
                print(f"         {'':15s}   {control.raw[50:]}")
    
    print(f"\n   SQL blocks found: {len(parser_result.sql_blocks)}")
    
    # Show SQL blocks
    if parser_result.sql_blocks:
        print(f"\n   🗃️  SQL Blocks:")
        for i, sql_block in enumerate(parser_result.sql_blocks, 1):
            print(f"     {i}. Lines {sql_block.start_line:3d}-{sql_block.end_line:3d}: ", end="")
            # Show first line of SQL
            first_line = sql_block.sql.split('\n')[0].strip()
            sql_type = first_line.split()[0].upper() if first_line else "UNKNOWN"
            print(f"{sql_type:10s} | {first_line[:40]}...")
    
    # Generate stored procedure
    print(f"\n⚙️  Generating Snowflake stored procedure...")
    generator = SnowflakeSPGenerator()
    
    procedure = generator.generate(
        parser_result=parser_result,
        procedure_name="PROC_ACCT_BALN_BKDT_ADJ_RULE_ISRT",
        original_bteq=sample_bteq
    )
    
    print(f"   ✅ Generated procedure: {procedure.name}")
    print(f"   Parameters: {len(procedure.parameters)}")
    if procedure.parameters:
        print("   📝 Parameters:")
        for param in procedure.parameters:
            print(f"      • {param}")
            
    print(f"   SQL lines: {len(procedure.sql.splitlines())}")
    print(f"   Warnings: {len(procedure.warnings)}")
    
    # Show warnings
    if procedure.warnings:
        print(f"\n⚠️  Conversion Warnings:")
        for i, warning in enumerate(procedure.warnings, 1):
            print(f"     {i:2d}. {warning}")
    
    # Show error handling features  
    if procedure.error_handling:
        print(f"\n✅ Error Handling Features:")
        for i, feature in enumerate(procedure.error_handling, 1):
            print(f"     {i:2d}. {feature}")
    
    # Show procedure preview (first 40 lines)
    print(f"\n📄 Generated Procedure Preview (first 40 lines):")
    print("="*80)
    sql_lines = procedure.sql.split('\n')
    for i, line in enumerate(sql_lines[:40], 1):
        print(f"{i:3d}| {line}")
    
    if len(sql_lines) > 40:
        print(f"    ... ({len(sql_lines) - 40} more lines)")
    print("="*80)
    
    # Save to file
    output_file = project_root / "generated_procedure_standalone.sql"
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(procedure.sql)
        print(f"\n💾 Complete procedure saved to: {output_file}")
    except Exception as e:
        print(f"\n❌ Could not save file: {e}")
    
    # Summary
    print(f"\n📊 GENERATION SUMMARY")
    print(f"{'='*50}")
    print(f"Input Analysis:")
    print(f"  • BTEQ lines:        {len(bteq_lines)}")
    print(f"  • Control statements: {len(parser_result.controls)}")
    print(f"  • SQL blocks:        {len(parser_result.sql_blocks)}")
    
    print(f"\nOutput Results:")
    print(f"  • Procedure name:    {procedure.name}")
    print(f"  • Parameters:        {len(procedure.parameters)}")
    print(f"  • Generated lines:   {len(procedure.sql.splitlines())}")
    print(f"  • Conversion warnings: {len(procedure.warnings)}")
    print(f"  • Error handling:    {len(procedure.error_handling)} features")
    
    # Analysis of conversion coverage
    control_types = set(c.type.name for c in parser_result.controls)
    supported_controls = {'RUN', 'IF_ERRORCODE', 'GOTO', 'LABEL', 'LOGOFF', 'COLLECT_STATS', 'CALL_SP'}
    unsupported_controls = control_types - supported_controls
    
    print(f"\nConversion Coverage:")
    print(f"  • Control types found: {', '.join(sorted(control_types))}")
    if unsupported_controls:
        print(f"  • Needs manual review: {', '.join(sorted(unsupported_controls))}")
    else:
        print(f"  • All controls supported ✅")
    
    print(f"\n✨ Standalone demo completed successfully!")
    print(f"\n🎯 Next Steps:")
    print(f"  1. Review generated procedure in: {output_file}")
    print(f"  2. Address any conversion warnings")
    print(f"  3. Test procedure in Snowflake environment")
    print(f"  4. Integrate with dbt workflows")
    
    return 0


if __name__ == "__main__":
    sys.exit(standalone_demo())
