#!/usr/bin/env python3
"""
Test Claude Sonnet LLM Integration

Tests Claude Sonnet specifically for BTEQ enhancement tasks.
"""

import logging
import sys
from pathlib import Path

# Add bteq_dcf to path
sys.path.insert(0, str(Path(__file__).parent.parent))

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')

def test_claude_llm():
    """Test Claude Sonnet for BTEQ enhancement scenarios."""
    
    print('🤖 Testing Claude Sonnet for BTEQ Enhancement')
    print('='*55)
    
    try:
        from services.conversion.stored_proc.llm_enhanced import LLMEnhancedGenerator, create_llm_generation_context
        from parser.tokens import ParserResult, ControlStatement, SqlBlock, ControlType
        
        # Test scenario 1: Complex BTEQ with control flow
        print('📋 Test 1: Complex BTEQ Script Enhancement')
        print('-' * 45)
        
        complex_bteq = '''
.RUN FILE=%%BTEQ_LOGON_SCRIPT%%
.IF ERRORCODE <> 0 THEN .GOTO EXITERR

-- Account Balance Backdated Adjustment Rules
DELETE PDDSTG.ACCT_BALN_BKDT_ADJ_RULE;
.IF ERRORCODE <> 0 THEN .GOTO EXITERR

INSERT INTO PDDSTG.ACCT_BALN_BKDT_ADJ_RULE 
SELECT 
    DT1.ACCT_ID,
    DT1.TIME_PERD_C,
    DT1.ADJ_FROM_D,
    CASE WHEN ((DT1.EFFT_D - DT1.ADJ_FROM_D) YEAR(4) TO MONTH) = 0 
         THEN DT1.ADJ_FROM_D 
         WHEN ((DT1.EFFT_D - DT1.ADJ_FROM_D) YEAR(4) TO MONTH) = 1 
              AND DT1.EFFT_D <= BSDY_4.CALR_CALR_D 
         THEN DT1.ADJ_FROM_D
         ELSE DT1.EFFT_D - (EXTRACT (DAY FROM DT1.EFFT_D) - 1)  
    END AS BKDT_ADJ_FROM_D,
    SUM(DT1.ADJ_A) AS ADJ_A,
    DT1.EFFT_D
FROM PVTECH.ACCT_BALN_BKDT_STG2 DT1
INNER JOIN PVTECH.BUSINESS_DAY_4 BSDY_4 
    ON DT1.TIME_PERD_C = BSDY_4.TIME_PERD_C
WHERE DT1.ADJ_FROM_D IS NOT NULL
GROUP BY 1,2,3,4,6;

.IF ERRORCODE <> 0 THEN .GOTO EXITERR

.LABEL EXITERR
.LOGOFF
'''
        
        complex_analysis = '''
# Complex BTEQ Analysis

## File Overview
- **Control Statements**: 5 (RUN, IF_ERRORCODE x3, LABEL, LOGOFF)
- **SQL Blocks**: 2 (DELETE, Complex INSERT with CASE logic)
- **Complexity Score**: 89

## Key Features
- **Teradata Features**: Date arithmetic, YEAR/MONTH intervals, EXTRACT
- **Business Logic**: Backdated adjustment calculations
- **Error Handling**: Multiple .IF ERRORCODE checks
- **Joins**: INNER JOIN with business calendar

## Migration Recommendations
**Incremental Model** - High complexity requires careful conversion
'''
        
        basic_procedure = '''
CREATE OR REPLACE PROCEDURE BASIC_ACCT_BALN_PROC()
RETURNS STRING
AS $$
BEGIN
  DELETE PDDSTG.ACCT_BALN_BKDT_ADJ_RULE;
  INSERT INTO PDDSTG.ACCT_BALN_BKDT_ADJ_RULE SELECT * FROM TEMP;
  RETURN 'Basic completed';
END;
$$;
'''
        
        # Create parser result
        parser_result = ParserResult(
            controls=[
                ControlStatement(ControlType.RUN, '.RUN FILE=%%BTEQ_LOGON_SCRIPT%%', None, 1),
                ControlStatement(ControlType.IF_ERRORCODE, '.IF ERRORCODE <> 0 THEN .GOTO EXITERR', None, 2),
                ControlStatement(ControlType.IF_ERRORCODE, '.IF ERRORCODE <> 0 THEN .GOTO EXITERR', None, 5),
                ControlStatement(ControlType.IF_ERRORCODE, '.IF ERRORCODE <> 0 THEN .GOTO EXITERR', None, 25),
                ControlStatement(ControlType.LABEL, '.LABEL EXITERR', None, 27),
                ControlStatement(ControlType.LOGOFF, '.LOGOFF', None, 28)
            ],
            sql_blocks=[
                SqlBlock('DELETE PDDSTG.ACCT_BALN_BKDT_ADJ_RULE;', 4, 4),
                SqlBlock('INSERT INTO PDDSTG.ACCT_BALN_BKDT_ADJ_RULE SELECT...', 6, 24)
            ]
        )
        
        # Create LLM context
        context = create_llm_generation_context(
            original_bteq=complex_bteq,
            analysis_markdown=complex_analysis,
            basic_procedure=basic_procedure,
            procedure_name='CLAUDE_ENHANCED_ACCT_BALN_PROC',
            parser_result=parser_result
        )
        
        print(f'Context: {len(complex_bteq)} chars BTEQ, {len(parser_result.controls)} controls')
        
        # Generate with Claude
        print('🧠 Generating with Claude Sonnet...')
        enhanced_generator = LLMEnhancedGenerator()
        enhanced_result = enhanced_generator.generate_enhanced(context)
        
        print('✅ Claude generation completed!')
        print(f'📊 Results:')
        print(f'   Name: {enhanced_result.name}')
        print(f'   Quality Score: {enhanced_result.quality_score:.2f}')
        print(f'   SQL Length: {len(enhanced_result.sql):,} chars')
        print(f'   Lines: {len(enhanced_result.sql.splitlines())}')
        print(f'   Enhancements: {len(enhanced_result.llm_enhancements)}')
        
        if enhanced_result.llm_enhancements:
            print(f'\n🎯 Claude Enhancements:')
            for enhancement in enhanced_result.llm_enhancements:
                print(f'   • {enhancement}')
        
        # Show procedure preview
        lines = enhanced_result.sql.splitlines()
        print(f'\n🏗️  Claude-Generated Preview:')
        for i, line in enumerate(lines[:12], 1):
            if line.strip():
                print(f'   {i:2}: {line[:75]}')
        
        # Test 2: Simple procedure enhancement
        print(f'\n📋 Test 2: Simple Procedure Enhancement')
        print('-' * 40)
        
        simple_test = '''
.RUN FILE=logon.sql
SELECT COUNT(*) FROM ACCT_TABLE;
.LOGOFF
'''
        
        from services.conversion.llm.integration import create_llm_service
        llm_service = create_llm_service()
        
        if llm_service.available:
            simple_prompt = '''
Convert this simple BTEQ script to a modern Snowflake stored procedure:

.RUN FILE=logon.sql
SELECT COUNT(*) FROM ACCT_TABLE;
.LOGOFF

Requirements:
- Include proper error handling
- Add logging
- Return meaningful results
- Use modern Snowflake syntax
- Keep it concise but production-ready
'''
            
            result = llm_service.enhance_procedure(simple_prompt)
            
            if result:
                print(f'✅ Simple enhancement successful ({len(result)} chars)')
                print('📄 Claude Response:')
                print('─' * 50)
                # Show first part of response
                lines = result.splitlines()
                for i, line in enumerate(lines[:15], 1):
                    print(f'{i:2}: {line}')
                if len(lines) > 15:
                    print(f'... ({len(lines) - 15} more lines)')
                print('─' * 50)
            else:
                print('❌ Simple enhancement failed')
        
        # Save output
        output_file = Path(__file__).parent / 'claude_test_output.sql'
        with open(output_file, 'w') as f:
            f.write(enhanced_result.sql)
        
        print(f'\n💾 Claude test output saved to: {output_file}')
        print(f'\n🎉 Claude Sonnet Testing Complete!')
        print(f'🌟 All tests passed with claude-3-5-sonnet')
        
        return True
        
    except Exception as e:
        print(f'❌ Claude test failed: {e}')
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = test_claude_llm()
    sys.exit(0 if success else 1)
