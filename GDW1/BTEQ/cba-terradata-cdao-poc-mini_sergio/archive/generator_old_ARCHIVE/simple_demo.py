#!/usr/bin/env python3
"""Simplified demo script showing BTEQ generator without external dependencies."""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

# Import only the lexer and generator components (no parser service dependencies)
from parser.tokens import ParserResult, ControlStatement, ControlType, SqlBlock
from parser.bteq_lexer import lex_bteq
from generator.snowflake_sp_generator import SnowflakeSPGenerator


def simple_demo():
    """Demonstrate the BTEQ generator with a simple example."""
    
    print("="*80)
    print("BTEQ TO SNOWFLAKE STORED PROCEDURE GENERATOR - SIMPLE DEMO")
    print("="*80)
    
    # Create a simple BTEQ example directly
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

.LABEL EXITERR
.LOGOFF
"""
    
    print(f"📖 Sample BTEQ Script:")
    print("-" * 50)
    print(sample_bteq.strip())
    
    # Parse BTEQ
    print(f"\n🔍 Parsing BTEQ content...")
    parser_result = lex_bteq(sample_bteq)
    
    print(f"   Control statements found: {len(parser_result.controls)}")
    for control in parser_result.controls:
        print(f"     • Line {control.line_no}: {control.type.name}")
        print(f"       Raw: {control.raw}")
    
    print(f"\n   SQL blocks found: {len(parser_result.sql_blocks)}")
    for i, sql_block in enumerate(parser_result.sql_blocks, 1):
        print(f"     • Block {i}: Lines {sql_block.start_line}-{sql_block.end_line}")
        # Show first line of SQL
        first_line = sql_block.sql.split('\n')[0].strip()
        print(f"       Starts with: {first_line}")
    
    # Generate stored procedure
    print(f"\n⚙️  Generating Snowflake stored procedure...")
    generator = SnowflakeSPGenerator()
    
    procedure = generator.generate(
        parser_result=parser_result,
        procedure_name="PROC_ACCT_BALN_ADJ_DEMO",
        original_bteq=sample_bteq
    )
    
    print(f"   ✅ Generated procedure: {procedure.name}")
    print(f"   Parameters: {len(procedure.parameters)}")
    print(f"   SQL lines: {len(procedure.sql.splitlines())}")
    print(f"   Warnings: {len(procedure.warnings)}")
    
    # Show warnings
    if procedure.warnings:
        print(f"\n⚠️  Conversion warnings:")
        for warning in procedure.warnings:
            print(f"     • {warning}")
    
    # Show error handling features  
    if procedure.error_handling:
        print(f"\n✅ Error handling features:")
        for feature in procedure.error_handling:
            print(f"     • {feature}")
    
    # Show generated procedure
    print(f"\n📄 Generated Snowflake Stored Procedure:")
    print("="*80)
    print(procedure.sql)
    print("="*80)
    
    # Save to file
    output_file = project_root / "simple_generated_procedure.sql"
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(procedure.sql)
    
    print(f"\n💾 Procedure saved to: {output_file}")
    
    # Summary
    print(f"\n📊 GENERATION SUMMARY")
    print(f"{'='*40}")
    print(f"Original BTEQ:     {len(parser_result.controls)} controls, {len(parser_result.sql_blocks)} SQL blocks")
    print(f"Generated proc:    {len(procedure.parameters)} parameters, {len(procedure.sql.splitlines())} lines")
    print(f"Conversion issues: {len(procedure.warnings)} warnings")
    
    print(f"\n✨ Simple demo completed successfully!")
    return 0


if __name__ == "__main__":
    sys.exit(simple_demo())
