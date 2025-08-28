#!/usr/bin/env python3
"""Demo script showing BTEQ to Snowflake generator in action."""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from parser.bteq_lexer import lex_bteq
from services.conversion.stored_proc.service import GeneratorService


def demo_generator():
    """Demonstrate the BTEQ generator with a real example."""
    
    # Path to sample BTEQ file
    bteq_file = project_root / "current_state" / "bteq_bteq" / "ACCT_BALN_BKDT_ADJ_RULE_ISRT.bteq"
    
    if not bteq_file.exists():
        print(f"BTEQ file not found: {bteq_file}")
        return 1
    
    print("="*80)
    print("BTEQ TO SNOWFLAKE STORED PROCEDURE GENERATOR DEMO")
    print("="*80)
    
    # Read BTEQ content
    print(f"📖 Reading BTEQ file: {bteq_file.name}")
    with open(bteq_file, 'r', encoding='utf-8') as f:
        bteq_content = f.read()
    
    print(f"   File size: {len(bteq_content)} characters")
    print(f"   Lines: {len(bteq_content.splitlines())}")
    
    # Parse BTEQ
    print(f"\n🔍 Parsing BTEQ content...")
    parser_result = lex_bteq(bteq_content)
    
    print(f"   Control statements found: {len(parser_result.controls)}")
    for control in parser_result.controls:
        print(f"     • Line {control.line_no}: {control.type.name} - {control.raw[:50]}...")
    
    print(f"   SQL blocks found: {len(parser_result.sql_blocks)}")
    for i, sql_block in enumerate(parser_result.sql_blocks, 1):
        print(f"     • Block {i}: Lines {sql_block.start_line}-{sql_block.end_line}")
        # Show first line of SQL
        first_line = sql_block.sql.split('\n')[0].strip()
        print(f"       {first_line[:60]}...")
    
    # Generate stored procedure
    print(f"\n⚙️  Generating Snowflake stored procedure...")
    generator = GeneratorService(enable_logging=False)
    
    result = generator.generate(
        parser_result=parser_result,
        original_bteq=bteq_content,
        procedure_name="PROC_ACCT_BALN_BKDT_ADJ_RULE_ISRT"
    )
    
    procedure = result.procedure
    
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
    
    # Run validation
    print(f"\n🔍 Running validation...")
    validation = generator.validate_generation(result)
    
    if validation["valid"]:
        print("   ✅ Validation passed")
    else:
        print("   ⚠️  Validation issues:")
        for issue in validation["issues"]:
            print(f"     • {issue}")
    
    if validation["recommendations"]:
        print("   💡 Recommendations:")
        for rec in validation["recommendations"]:
            print(f"     • {rec}")
    
    # Show procedure preview
    print(f"\n📄 Generated procedure preview (first 30 lines):")
    print("-" * 80)
    sql_lines = procedure.sql.split('\n')
    for i, line in enumerate(sql_lines[:30], 1):
        print(f"{i:2d}| {line}")
    
    if len(sql_lines) > 30:
        print(f"... ({len(sql_lines) - 30} more lines)")
    
    # Save to file
    output_file = project_root / "generated_procedure_demo.sql"
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(procedure.sql)
    
    print(f"\n💾 Full procedure saved to: {output_file}")
    
    # Summary
    print(f"\n📊 GENERATION SUMMARY")
    print(f"{'='*40}")
    metadata = result.generation_metadata
    print(f"Original BTEQ:     {metadata['parser_stats']['control_statements']} controls, {metadata['parser_stats']['sql_blocks']} SQL blocks")
    print(f"Generated proc:    {len(procedure.parameters)} parameters, {len(procedure.sql.splitlines())} lines")
    print(f"Conversion issues: {len(procedure.warnings)} warnings")
    print(f"Manual review:     {'Required' if validation['summary']['requires_manual_review'] else 'Optional'}")
    
    print(f"\n✨ Demo completed successfully!")
    return 0


if __name__ == "__main__":
    sys.exit(demo_generator())
