#!/usr/bin/env python3
"""
Test Snowflake Connection and Cortex Integration

Tests the Snowflake connection, Cortex availability, and LLM integration.
"""

import logging
import sys
from pathlib import Path

# Add bteq_dcf to path
sys.path.insert(0, str(Path(__file__).parent.parent))

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')

def test_snowflake_connection():
    """Test Snowflake connection and Cortex integration."""
    
    print('🔗 Testing Snowflake Connection with pupad_svc credentials...')
    print('='*60)
    
    try:
        from bteq_dcf.utils.database import get_connection_manager
        
        # Reset connection manager to pick up any changes
        import bteq_dcf.utils.database as snowflake_connection
        snowflake_connection._connection_manager = None
        
        # Get connection manager
        connection_manager = get_connection_manager()
        
        # Test basic connection
        print('📡 Testing basic connection...')
        if connection_manager.test_connection():
            print('✅ Basic connection successful!')
            
            # Test Cortex availability  
            print('🤖 Testing Cortex availability...')
            if connection_manager.test_cortex_availability():
                print('✅ Cortex functions available!')
                
                # Test LLM service integration
                print('🧠 Testing LLM service integration...')
                from services.conversion.llm.integration import create_llm_service
                
                llm_service = create_llm_service()
                print(f'LLM Service Available: {llm_service.available}')
                
                if llm_service.available:
                    print('🚀 Testing Claude Sonnet LLM call...')
                    test_prompt = '''Create a simple SQL stored procedure that:
1. Takes a table name as parameter
2. Returns the row count
3. Includes error handling
4. Uses proper Snowflake syntax
Keep it concise (under 20 lines).'''
                    
                    result = llm_service.enhance_procedure(test_prompt)
                    
                    if result:
                        print(f'✅ Claude Sonnet Response ({len(result)} chars):')
                        print('─' * 50)
                        print(result[:500] + '...' if len(result) > 500 else result)
                        print('─' * 50)
                        print('🎉 Claude Sonnet integration working!')
                    else:
                        print('❌ No response from Claude Sonnet')
                        
                    # Test additional functions
                    print('\n🔍 Testing additional LLM functions...')
                    
                    # Test summarization
                    test_analysis = '''
                    This BTEQ script contains complex business logic with multiple control statements.
                    It performs data transformation operations on account balance data with error handling.
                    The script includes DELETE, INSERT operations with complex CASE logic and date arithmetic.
                    Migration complexity is high due to Teradata-specific functions and control flow patterns.
                    '''
                    
                    summary = llm_service.summarize_analysis(test_analysis)
                    print(f'📄 Summary: {summary[:100]}...')
                    
                    # Test complexity assessment
                    test_bteq = '''
                    .RUN FILE=%%BTEQ_LOGON_SCRIPT%%
                    .IF ERRORCODE <> 0 THEN .GOTO EXITERR
                    DELETE TABLE;
                    INSERT INTO TABLE SELECT * FROM SOURCE;
                    .LABEL EXITERR
                    '''
                    
                    complexity = llm_service.assess_migration_complexity(test_bteq)
                    print(f'🎯 Complexity: {complexity[:100]}...')
                    
                else:
                    print('❌ LLM service not available')
            else:
                print('❌ Cortex functions not available')
        else:
            print('❌ Basic connection failed')
            
    except Exception as e:
        print(f'❌ Test failed: {e}')
        import traceback
        traceback.print_exc()
        return False
    
    return True


if __name__ == "__main__":
    success = test_snowflake_connection()
    sys.exit(0 if success else 1)
